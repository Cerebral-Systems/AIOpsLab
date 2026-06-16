# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Run exactly one AIOpsLab problem (or a cluster infra setup/teardown step).

This is the *killable unit* invoked as a subprocess by ``run_parallel.py``. Each
invocation targets whatever cluster the parent points it at via the environment
(``KUBECONFIG`` and/or ``AIOPSLAB_CLUSTER``) -- this script never selects a
cluster itself.

It emits a single machine-readable result line to stdout so the parent can
capture the outcome without having to guess the session-JSON filename:

    __AIOPS_RESULT__ {"status": "...", "session_id": "...", "results": {...}, ...}

Usage (run a problem):
    python scripts/parallel/run_one.py --problem <id> --agent gpt \
        --max-steps 30 --results-dir data/results/parallel_xxx

Usage (shared-infra lifecycle for --reuse-infra mode):
    python scripts/parallel/run_one.py --setup-infra
    python scripts/parallel/run_one.py --teardown-infra
"""

import argparse
import asyncio
import json
import sys
import time
import traceback
from pathlib import Path

# Make the repo root importable when invoked as a standalone script.
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

RESULT_SENTINEL = "__AIOPS_RESULT__"


def emit(payload: dict):
    """Print the single machine-readable result line for the parent to parse."""
    print(f"{RESULT_SENTINEL} {json.dumps(payload)}", flush=True)


def build_agent(agent_name: str):
    """Instantiate an agent by registry name (mirrors service.py defaults)."""
    from clients.registry import AgentRegistry

    registry = AgentRegistry()
    agent_cls = registry.get_agent(agent_name)
    if agent_cls is None:
        raise ValueError(
            f"Agent '{agent_name}' not registered. "
            f"Available: {registry.get_agent_ids()}"
        )
    # vLLM takes generation params; everything else is no-arg.
    return agent_cls()


async def _run_problem(problem_id: str, agent_name: str, max_steps: int, results_dir):
    """Run a single problem end-to-end inside one event loop.

    init_problem() may schedule an async workload via asyncio.create_task(), which
    requires a running loop -- so unlike service.py we run init + start in the
    *same* loop here.
    """
    from aiopslab.orchestrator import Orchestrator

    orch = Orchestrator(results_dir=results_dir)
    agent = build_agent(agent_name)
    orch.register_agent(agent, name=f"{agent_name}-agent")

    problem_desc, instructs, apis = orch.init_problem(problem_id)
    agent.init_context(problem_desc, instructs, apis)
    await orch.start_problem(max_steps=max_steps)
    return orch


def run_problem(args) -> int:
    results_dir = Path(args.results_dir) if args.results_dir else None
    start = time.time()
    try:
        orch = asyncio.run(
            _run_problem(args.problem, args.agent, args.max_steps, results_dir)
        )
        summary = orch.session.to_dict()
        results = summary.get("results") or {}
        emit(
            {
                "status": "completed",
                "problem_id": args.problem,
                "session_id": summary.get("session_id"),
                "results": results,
                "duration": time.time() - start,
            }
        )
        return 0
    except Exception as e:  # noqa: BLE001 - a failed problem must not kill the pool
        traceback.print_exc()
        emit(
            {
                "status": "error",
                "problem_id": args.problem,
                "error_type": type(e).__name__,
                "error": str(e),
                "duration": time.time() - start,
            }
        )
        return 1


def run_infra(setup: bool) -> int:
    """Install or uninstall the shared cluster infra (OpenEBS + Prometheus)."""
    from aiopslab.orchestrator import Orchestrator

    orch = Orchestrator()
    try:
        if setup:
            orch.setup_cluster_infra()
        else:
            orch.teardown_cluster_infra()
        emit({"status": "completed", "infra": "setup" if setup else "teardown"})
        return 0
    except Exception as e:  # noqa: BLE001
        traceback.print_exc()
        emit({"status": "error", "error_type": type(e).__name__, "error": str(e)})
        return 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a single AIOpsLab problem.")
    parser.add_argument("--problem", help="Problem ID to run.")
    parser.add_argument("--agent", default="gpt", help="Agent registry name.")
    parser.add_argument("--max-steps", type=int, default=30)
    parser.add_argument("--results-dir", default=None)
    parser.add_argument(
        "--setup-infra",
        action="store_true",
        help="Install shared infra (OpenEBS + Prometheus) on the active cluster, then exit.",
    )
    parser.add_argument(
        "--teardown-infra",
        action="store_true",
        help="Uninstall shared infra from the active cluster, then exit.",
    )
    args = parser.parse_args()

    if args.setup_infra:
        return run_infra(setup=True)
    if args.teardown_infra:
        return run_infra(setup=False)
    if not args.problem:
        parser.error("--problem is required unless --setup-infra/--teardown-infra is given")
    return run_problem(args)


if __name__ == "__main__":
    sys.exit(main())
