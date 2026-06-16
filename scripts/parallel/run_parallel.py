# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Run the AIOpsLab benchmark across many kind clusters in parallel.

Architecture
------------
* One *worker per kind cluster*; each worker pulls problems off a shared queue
  (dynamic scheduling, so a slow problem never strands an idle worker).
* Each problem runs as a killable subprocess (``run_one.py``) pinned to its
  worker's cluster via ``KUBECONFIG`` + ``AIOPSLAB_CLUSTER``.
* ``--reuse-infra`` installs OpenEBS + Prometheus *once per cluster* and tells
  each problem run to skip the per-problem install/teardown -- a large constant
  speedup on top of the W-way parallelism.

Provision clusters first with ``scripts/parallel/setup_clusters.py``.

Examples
--------
    # Dry run -- resolve clusters + problems and print the schedule, touch nothing
    python scripts/parallel/run_parallel.py --workers 4 --problems all --dry-run

    # Real run, GPT agent, 4 clusters, reuse shared infra
    python scripts/parallel/run_parallel.py --agent gpt --workers 4 \
        --problems detection --reuse-infra --max-steps 30
"""

import argparse
import asyncio
import json
import os
import signal
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

RUN_ONE = Path(__file__).resolve().parent / "run_one.py"
RESULT_SENTINEL = "__AIOPS_RESULT__"

# Live child processes, killed on Ctrl-C / fatal error.
_LIVE_PROCS: "set[asyncio.subprocess.Process]" = set()

try:
    from rich.console import Console
    from rich.table import Table

    _console = Console()
except Exception:  # pragma: no cover - rich is a project dep but stay defensive
    _console = None


def log(msg: str):
    if _console:
        _console.print(msg)
    else:
        print(msg, flush=True)


# --------------------------------------------------------------------------- #
# Resolution: clusters and problems
# --------------------------------------------------------------------------- #
def default_workers() -> int:
    """~1 cluster per 8 GB RAM, capped at 4; falls back to 4 if unknown."""
    try:
        import psutil

        gb = psutil.virtual_memory().total / (1024**3)
        return max(1, min(4, int(gb // 8)))
    except Exception:
        try:
            gb = (os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")) / (1024**3)
            return max(1, min(4, int(gb // 8)))
        except Exception:
            return 4


def resolve_clusters(args) -> "list[dict]":
    if args.clusters:
        names = [c.strip() for c in args.clusters.split(",") if c.strip()]
    else:
        names = [f"{args.cluster_prefix}-{i}" for i in range(args.workers)]
    kube_dir = Path(args.kubeconfig_dir)
    return [
        {"name": n, "kubeconfig": str((kube_dir / f"{n}.kubeconfig").resolve())}
        for n in names
    ]


def resolve_problems(spec: str) -> "list[str]":
    """Resolve --problems into a concrete, registry-validated list of IDs.

    Accepts: ``all`` | a file path (one ID per line) | a comma-separated list of
    IDs | a substring filter (e.g. ``detection``, ``misconfig_app``).
    """
    from aiopslab.orchestrator.problems.registry import ProblemRegistry

    registry = ProblemRegistry()
    all_ids = registry.get_problem_ids()
    all_set = set(all_ids)

    if spec == "all":
        return all_ids

    # File of IDs?
    p = Path(spec)
    if p.exists() and p.is_file():
        ids = []
        for line in p.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                ids.append(line)
    elif "," in spec:
        ids = [s.strip() for s in spec.split(",") if s.strip()]
    else:
        # Exact ID, else substring filter.
        if spec in all_set:
            return [spec]
        ids = [i for i in all_ids if spec in i]
        if not ids:
            raise SystemExit(
                f"--problems '{spec}' matched no problem IDs. "
                f"Use 'all', a filter substring, a comma-list, or a file."
            )
        return ids

    unknown = [i for i in ids if i not in all_set]
    if unknown:
        log(f"[yellow]Warning: {len(unknown)} unknown problem ID(s) ignored: {unknown}[/]"
            if _console else f"Warning: unknown problem IDs ignored: {unknown}")
    valid = [i for i in ids if i in all_set]
    if not valid:
        raise SystemExit("No valid problem IDs to run.")
    return valid


# --------------------------------------------------------------------------- #
# Outcome derivation (keys differ across task types)
# --------------------------------------------------------------------------- #
def derive_outcome(rec: dict) -> str:
    if rec.get("status") != "completed":
        return rec.get("status", "unknown")  # error / timeout
    results = rec.get("results") or {}
    if "success" in results:
        return "pass" if results["success"] else "fail"
    da = results.get("Detection Accuracy")
    if da is not None:
        s = str(da).lower()
        return "pass" if s == "correct" else ("invalid" if "invalid" in s else "fail")
    la = results.get("Localization Accuracy")
    if la is not None:
        try:
            la = float(la)
        except (TypeError, ValueError):
            return "unknown"
        return "pass" if la >= 100.0 else ("partial" if la > 0 else "fail")
    return "unknown"  # e.g. analysis tasks with bespoke keys


def score_str(rec: dict) -> str:
    results = rec.get("results") or {}
    bits = []
    for k in ("Detection Accuracy", "Localization Accuracy", "success"):
        if k in results:
            bits.append(f"{k.split()[0]}={results[k]}")
    for tk in ("TTD", "TTL", "TTA", "TTM"):
        if tk in results:
            try:
                bits.append(f"{tk}={float(results[tk]):.0f}s")
            except (TypeError, ValueError):
                pass
            break
    return ", ".join(bits) if bits else "-"


# --------------------------------------------------------------------------- #
# Subprocess execution
# --------------------------------------------------------------------------- #
async def _exec(cmd: "list[str]", env: dict, timeout: float, log_path: Path):
    """Run a subprocess, capturing combined output to log_path. Returns (rc, sentinel|None, timed_out)."""
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        env=env,
        start_new_session=True,  # own process group so we can kill the whole tree
    )
    _LIVE_PROCS.add(proc)
    timed_out = False
    try:
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        timed_out = True
        _kill_tree(proc)
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=30)
        except (asyncio.TimeoutError, ProcessLookupError):
            out = b""
    finally:
        _LIVE_PROCS.discard(proc)

    text = (out or b"").decode("utf-8", errors="replace")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(text)

    sentinel = None
    for line in text.splitlines():
        if line.startswith(RESULT_SENTINEL):
            try:
                sentinel = json.loads(line[len(RESULT_SENTINEL):].strip())
            except json.JSONDecodeError:
                sentinel = None
    return proc.returncode, sentinel, timed_out


def _kill_tree(proc):
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        try:
            proc.kill()
        except ProcessLookupError:
            pass


async def run_problem(pid, cluster, env, args, run_dir) -> dict:
    """Run one problem on one cluster, with retries. Returns a result record."""
    log_path = run_dir / "logs" / f"{pid}.log"
    last = None
    for attempt in range(1, args.retries + 2):  # retries=1 -> up to 2 attempts
        t0 = time.time()
        cmd = [
            args.python, str(RUN_ONE),
            "--problem", pid,
            "--agent", args.agent,
            "--max-steps", str(args.max_steps),
            "--results-dir", str(run_dir),
        ]
        rc, sentinel, timed_out = await _exec(cmd, env, args.timeout, log_path)
        wall = time.time() - t0

        if timed_out:
            rec = {"status": "timeout", "problem_id": pid, "duration": wall}
        elif sentinel is not None:
            rec = sentinel
            rec.setdefault("problem_id", pid)
            rec["wall"] = wall
        else:
            rec = {
                "status": "error", "problem_id": pid, "duration": wall,
                "error": f"subprocess exited rc={rc} with no result line (see {log_path})",
            }
        rec["cluster"] = cluster["name"]
        rec["attempt"] = attempt
        last = rec
        if rec.get("status") == "completed":
            break
        if attempt <= args.retries:
            log(f"  [yellow]retry[/] {pid} (attempt {attempt} {rec.get('status')}) "
                f"on {cluster['name']}" if _console
                else f"  retry {pid} (attempt {attempt} {rec.get('status')})")
    return last


def _base_env(cluster, args) -> dict:
    env = os.environ.copy()
    env["KUBECONFIG"] = cluster["kubeconfig"]
    env["AIOPSLAB_CLUSTER"] = cluster["name"]
    return env


async def worker(cluster, queue: asyncio.Queue, results: list, args, run_dir, state):
    env = _base_env(cluster, args)

    if args.reuse_infra:
        log(f"[cyan]{cluster['name']}[/]: installing shared infra (OpenEBS + Prometheus)…"
            if _console else f"{cluster['name']}: installing shared infra…")
        rc, sentinel, timed_out = await _exec(
            [args.python, str(RUN_ONE), "--setup-infra"],
            env, args.infra_timeout, run_dir / "logs" / f"_infra_setup_{cluster['name']}.log",
        )
        if (sentinel or {}).get("status") != "completed":
            log(f"[red]{cluster['name']}: infra setup failed; its problems will likely error.[/]"
                if _console else f"{cluster['name']}: infra setup FAILED")
        # Tell each problem run to reuse the now-warm infra.
        env["AIOPSLAB_SKIP_INFRA_SETUP"] = "true"
        env["AIOPSLAB_SKIP_INFRA_TEARDOWN"] = "true"

    while True:
        try:
            pid = queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        state["started"] += 1
        idx = state["started"]
        log(f"[dim]({idx}/{state['total']})[/] ▶ {pid}  →  {cluster['name']}"
            if _console else f"({idx}/{state['total']}) start {pid} -> {cluster['name']}")
        rec = await run_problem(pid, cluster, env, args, run_dir)
        results.append(rec)
        state["done"] += 1
        outcome = derive_outcome(rec)
        mark = {"pass": "[green]✓[/]", "fail": "[red]✗[/]", "timeout": "[red]⏱[/]",
                "error": "[red]✗[/]"}.get(outcome, "[yellow]•[/]")
        log(f"{mark} [{state['done']}/{state['total']}] {pid}  "
            f"({outcome}, {rec.get('wall', rec.get('duration', 0)):.0f}s)"
            if _console else
            f"[{state['done']}/{state['total']}] {pid} {outcome} "
            f"{rec.get('wall', rec.get('duration', 0)):.0f}s")

    if args.reuse_infra and not args.keep_infra:
        await _exec(
            [args.python, str(RUN_ONE), "--teardown-infra"],
            _base_env(cluster, args), args.infra_timeout,
            run_dir / "logs" / f"_infra_teardown_{cluster['name']}.log",
        )


# --------------------------------------------------------------------------- #
# Aggregation / reporting
# --------------------------------------------------------------------------- #
def write_summary(results, clusters, args, run_dir, wall_clock):
    sum_problem_time = sum(r.get("wall", r.get("duration", 0) or 0) for r in results)
    counts = {}
    for r in results:
        o = derive_outcome(r)
        counts[o] = counts.get(o, 0) + 1
    passed = counts.get("pass", 0)
    completed = sum(1 for r in results if r.get("status") == "completed")

    summary = {
        "agent": args.agent,
        "workers": len(clusters),
        "clusters": [c["name"] for c in clusters],
        "reuse_infra": args.reuse_infra,
        "max_steps": args.max_steps,
        "num_problems": len(results),
        "outcomes": counts,
        "passed": passed,
        "completed": completed,
        "wall_clock_s": round(wall_clock, 1),
        "summed_problem_s": round(sum_problem_time, 1),
        "speedup_x": round(sum_problem_time / wall_clock, 2) if wall_clock else None,
        "results": sorted(results, key=lambda r: r.get("problem_id", "")),
    }
    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2))
    return summary


def print_report(summary, run_dir):
    rows = sorted(summary["results"], key=lambda r: r.get("problem_id", ""))
    if _console:
        table = Table(title=f"AIOpsLab parallel run — {summary['agent']}", show_lines=False)
        for col in ("#", "problem", "outcome", "score", "dur(s)", "try", "cluster"):
            table.add_column(col, overflow="fold")
        color = {"pass": "green", "fail": "red", "timeout": "red", "error": "red"}
        for i, r in enumerate(rows, 1):
            o = derive_outcome(r)
            table.add_row(
                str(i), r.get("problem_id", "?"),
                f"[{color.get(o, 'yellow')}]{o}[/]", score_str(r),
                f"{r.get('wall', r.get('duration', 0) or 0):.0f}",
                str(r.get("attempt", 1)), r.get("cluster", "-"),
            )
        _console.print(table)
    else:
        for i, r in enumerate(rows, 1):
            print(f"{i:>3} {r.get('problem_id'):45} {derive_outcome(r):8} "
                  f"{r.get('wall', r.get('duration', 0) or 0):6.0f}s {r.get('cluster')}")

    log("")
    log(f"[bold]Passed:[/] {summary['passed']}/{summary['num_problems']}   "
        f"[bold]Completed:[/] {summary['completed']}/{summary['num_problems']}   "
        f"Outcomes: {summary['outcomes']}" if _console else
        f"Passed: {summary['passed']}/{summary['num_problems']}  "
        f"Completed: {summary['completed']}/{summary['num_problems']}  {summary['outcomes']}")
    log(f"[bold]Wall clock:[/] {summary['wall_clock_s']}s   "
        f"[bold]Summed problem time:[/] {summary['summed_problem_s']}s   "
        f"[bold green]Speedup: {summary['speedup_x']}×[/]" if _console else
        f"Wall clock: {summary['wall_clock_s']}s  Summed: {summary['summed_problem_s']}s  "
        f"Speedup: {summary['speedup_x']}x")
    log(f"[dim]Results + logs: {run_dir}[/]" if _console else f"Results + logs: {run_dir}")


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
async def run(args, clusters, problems, run_dir):
    queue: asyncio.Queue = asyncio.Queue()
    for p in problems:
        queue.put_nowait(p)
    results: list = []
    state = {"total": len(problems), "started": 0, "done": 0}

    workers = [
        asyncio.create_task(worker(c, queue, results, args, run_dir, state))
        for c in clusters
    ]
    try:
        await asyncio.gather(*workers)
    finally:
        for proc in list(_LIVE_PROCS):
            _kill_tree(proc)
    return results


def preflight(clusters, args):
    """Verify kubeconfig files exist before launching (skipped for --dry-run)."""
    missing = [c for c in clusters if not Path(c["kubeconfig"]).exists()]
    if missing:
        names = ", ".join(c["name"] for c in missing)
        raise SystemExit(
            f"Missing kubeconfig(s) for: {names}\n"
            f"Provision clusters first:\n"
            f"  python scripts/parallel/setup_clusters.py --workers {args.workers}"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Run the AIOpsLab benchmark across multiple kind clusters in parallel."
    )
    parser.add_argument("--agent", default="gpt", help="Agent registry name (default: gpt).")
    parser.add_argument("--workers", type=int, default=None,
                        help="Number of clusters/workers (default: auto from RAM, max 4).")
    parser.add_argument("--problems", default="all",
                        help="'all' | filter substring | comma-list | file path.")
    parser.add_argument("--max-steps", type=int, default=30)
    parser.add_argument("--reuse-infra", action="store_true",
                        help="Install OpenEBS + Prometheus once per cluster instead of per problem.")
    parser.add_argument("--keep-infra", action="store_true",
                        help="With --reuse-infra, do not tear down shared infra at the end.")
    parser.add_argument("--timeout", type=float, default=1800,
                        help="Per-problem timeout in seconds (default: 1800).")
    parser.add_argument("--infra-timeout", type=float, default=900,
                        help="Timeout for shared-infra setup/teardown (default: 900).")
    parser.add_argument("--retries", type=int, default=1,
                        help="Retries for a failed/timed-out problem (default: 1).")
    parser.add_argument("--clusters", default=None,
                        help="Explicit comma-separated cluster names (overrides --workers).")
    parser.add_argument("--cluster-prefix", default="aiops",
                        help="Cluster name prefix when deriving from --workers (default: aiops).")
    parser.add_argument("--kubeconfig-dir", default=str(REPO_ROOT / ".aiops-clusters"),
                        help="Directory holding <cluster>.kubeconfig files.")
    parser.add_argument("--results-dir", default=None,
                        help="Parent results dir (default: data/results/parallel_<ts>).")
    parser.add_argument("--python", default=sys.executable,
                        help="Python interpreter for worker subprocesses.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Resolve clusters + problems, print the plan, and exit.")
    parser.add_argument("--list-problems", action="store_true",
                        help="Print all available problem IDs and exit.")
    args = parser.parse_args()

    if args.list_problems:
        from aiopslab.orchestrator.problems.registry import ProblemRegistry
        for pid in ProblemRegistry().get_problem_ids():
            print(pid)
        return

    if args.workers is None:
        args.workers = default_workers()

    clusters = resolve_clusters(args)
    problems = resolve_problems(args.problems)
    args.workers = len(clusters)

    ts = time.strftime("%Y%m%d_%H%M%S")
    run_dir = Path(args.results_dir) if args.results_dir else (
        REPO_ROOT / "data" / "results" / f"parallel_{ts}"
    )

    log(f"[bold]AIOpsLab parallel runner[/]" if _console else "AIOpsLab parallel runner")
    log(f"  agent={args.agent}  workers={len(clusters)}  problems={len(problems)}  "
        f"max_steps={args.max_steps}  reuse_infra={args.reuse_infra}")
    log(f"  clusters: {', '.join(c['name'] for c in clusters)}")
    log(f"  results:  {run_dir}")

    if args.dry_run:
        log("\n[bold yellow]DRY RUN[/] — no clusters touched. Schedule:" if _console
            else "\nDRY RUN — no clusters touched. Schedule:")
        for i, p in enumerate(problems):
            log(f"  {i+1:>3}. {p}   (→ {clusters[i % len(clusters)]['name']} if idle)")
        log(f"\n{len(problems)} problems across {len(clusters)} workers "
            f"(dynamic queue; assignment above is illustrative).")
        return

    preflight(clusters, args)
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "config.json").write_text(json.dumps(vars(args), indent=2, default=str))

    t0 = time.time()
    try:
        results = asyncio.run(run(args, clusters, problems, run_dir))
    except KeyboardInterrupt:
        for proc in list(_LIVE_PROCS):
            _kill_tree(proc)
        log("[red]Interrupted — killed running problems.[/]" if _console else "Interrupted.")
        raise
    wall = time.time() - t0

    summary = write_summary(results, clusters, args, run_dir, wall)
    print_report(summary, run_dir)


if __name__ == "__main__":
    main()
