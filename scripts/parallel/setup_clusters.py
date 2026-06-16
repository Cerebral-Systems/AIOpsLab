# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Provision (or tear down) N kind clusters for parallel AIOpsLab benchmarking.

Each cluster gets an isolated kubeconfig under ``--kubeconfig-dir`` (default
``.aiops-clusters/``) whose current-context is ``kind-<name>``. ``run_parallel.py``
points one worker at each via ``KUBECONFIG``, so raw ``kubectl``/``helm`` shell-outs
*and* the Python client all target the right cluster.

Each cluster is control-plane + worker = 2 Docker containers running a full
microservice app, so budget ~6-8 GB RAM per cluster.

Examples
--------
    python scripts/parallel/setup_clusters.py --workers 4            # create aiops-0..3
    python scripts/parallel/setup_clusters.py --action list
    python scripts/parallel/setup_clusters.py --action delete --workers 4
    python scripts/parallel/setup_clusters.py --action reset --workers 4   # delete + recreate
"""

import argparse
import platform
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def run(cmd, **kw):
    print(f"$ {' '.join(cmd)}", flush=True)
    return subprocess.run(cmd, **kw)


def require(tool: str):
    if shutil.which(tool) is None:
        sys.exit(f"Error: '{tool}' not found on PATH. Please install it first.")


def default_config() -> Path:
    machine = platform.machine().lower()
    arch = "arm" if machine in {"arm64", "aarch64"} else "x86"
    cfg = REPO_ROOT / "kind" / f"kind-config-{arch}.yaml"
    if not cfg.exists():
        sys.exit(f"kind config not found: {cfg}")
    return cfg


def existing_clusters() -> "set[str]":
    out = subprocess.run(["kind", "get", "clusters"], capture_output=True, text=True)
    return {ln.strip() for ln in out.stdout.splitlines() if ln.strip()}


def cluster_names(args) -> "list[str]":
    if args.clusters:
        return [c.strip() for c in args.clusters.split(",") if c.strip()]
    return [f"{args.cluster_prefix}-{i}" for i in range(args.workers)]


def create(names, config, kube_dir: Path, load_images: bool):
    require("kind")
    require("kubectl")
    kube_dir.mkdir(parents=True, exist_ok=True)
    present = existing_clusters()

    for name in names:
        if name in present:
            print(f"✓ cluster '{name}' already exists — skipping create.")
        else:
            r = run(["kind", "create", "cluster", "--name", name, "--config", str(config)])
            if r.returncode != 0:
                sys.exit(f"Failed to create cluster '{name}'.")

        kubeconfig = kube_dir / f"{name}.kubeconfig"
        run(["kind", "export", "kubeconfig", "--name", name,
             "--kubeconfig", str(kubeconfig)])

        if load_images:
            script = REPO_ROOT / "kind" / "load_images.sh"
            print(f"Loading images into '{name}' (this can take a while)…")
            run(["bash", str(script), name], cwd=str(REPO_ROOT))

    print("\nProvisioned clusters:")
    for name in names:
        print(f"  {name}  ->  {kube_dir / f'{name}.kubeconfig'}")
    print(
        "\nNext:\n"
        f"  python scripts/parallel/run_parallel.py --workers {len(names)} "
        f"--problems all --reuse-infra --dry-run"
    )


def delete(names):
    require("kind")
    present = existing_clusters()
    for name in names:
        if name in present:
            run(["kind", "delete", "cluster", "--name", name])
        else:
            print(f"cluster '{name}' not found — skipping.")


def list_clusters(names):
    present = existing_clusters()
    print("kind clusters present:")
    for c in sorted(present):
        mark = " (target)" if c in names else ""
        print(f"  {c}{mark}")
    missing = [n for n in names if n not in present]
    if missing:
        print("missing targets:", ", ".join(missing))


def main():
    parser = argparse.ArgumentParser(description="Provision kind clusters for parallel AIOpsLab.")
    parser.add_argument("--action", choices=["create", "delete", "list", "reset"],
                        default="create")
    parser.add_argument("--workers", type=int, default=2,
                        help="Number of clusters to create (named <prefix>-0..N-1).")
    parser.add_argument("--clusters", default=None,
                        help="Explicit comma-separated cluster names (overrides --workers).")
    parser.add_argument("--cluster-prefix", default="aiops")
    parser.add_argument("--kubeconfig-dir", default=str(REPO_ROOT / ".aiops-clusters"))
    parser.add_argument("--config", default=None, help="kind config YAML (default: auto by arch).")
    parser.add_argument("--load-images", action="store_true",
                        help="Also load kind/images.txt into each cluster (only needed if the "
                             "node image does not already bake them in).")
    args = parser.parse_args()

    names = cluster_names(args)
    kube_dir = Path(args.kubeconfig_dir)
    config = Path(args.config) if args.config else default_config()

    if args.action == "list":
        list_clusters(names)
    elif args.action == "delete":
        delete(names)
    elif args.action == "reset":
        delete(names)
        create(names, config, kube_dir, args.load_images)
    else:
        create(names, config, kube_dir, args.load_images)


if __name__ == "__main__":
    main()
