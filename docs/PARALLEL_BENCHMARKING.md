# Parallel benchmarking (kind-per-worker)

Run the AIOpsLab benchmark across **multiple kind clusters at once** to cut
wall-clock time. One worker per cluster pulls problems off a shared queue; each
problem runs as an isolated, killable subprocess pinned to its worker's cluster.

Two multiplicative speedups:

1. **W×** from running `W` problems concurrently (one per cluster).
2. A **constant factor** from `--reuse-infra`, which installs OpenEBS +
   Prometheus *once per cluster* instead of installing **and uninstalling** them
   on every single problem (the per-problem OpenEBS churn — including waiting for
   the `openebs` namespace to delete — is a large slice of today's overhead).

```
T_parallel ≈ (Σ per-problem time) / W  +  one-time cluster provisioning  +  tail
```

---

## Why parallel runs needed a fix

Each AIOpsLab run mutates **cluster-global** state (OpenEBS install/uninstall,
the shared Prometheus, a fixed app namespace), so two runs on one cluster corrupt
each other. We isolate by giving **each worker its own kind cluster** and its own
kubeconfig.

The catch: raw `kubectl`/`helm` shell-outs in the codebase don't pass
`--context`, so they follow whatever `KUBECONFIG` points at. The runner therefore
sets a **per-worker `KUBECONFIG`** (a kubeconfig whose current-context is
`kind-<name>`). With that, raw `kubectl`, `helm`, **and** the Python client all
target the right cluster. `KubeCtl` was hardened to honor that current-context
when `KUBECONFIG` is set ([aiopslab/service/kubectl.py](../aiopslab/service/kubectl.py)).

---

## 1. Provision clusters

Each cluster is control-plane + worker = **2 Docker containers** running a full
microservice app. Budget **~6–8 GB RAM per cluster**.

```bash
# Create aiops-0 .. aiops-3 (auto-detects arm/x86 kind config)
python scripts/parallel/setup_clusters.py --workers 4

# Inspect / tear down / recreate
python scripts/parallel/setup_clusters.py --action list   --workers 4
python scripts/parallel/setup_clusters.py --action delete --workers 4
python scripts/parallel/setup_clusters.py --action reset  --workers 4   # delete + recreate
```

Kubeconfigs are written to `.aiops-clusters/<name>.kubeconfig`.

> The bundled kind node image bakes in the app images. If you use a vanilla node
> image, add `--load-images` to also load `kind/images.txt` into each cluster.

---

## 2. Run the benchmark

```bash
# See the plan first — resolves clusters + problems, touches no cluster
python scripts/parallel/run_parallel.py --workers 4 --problems all --dry-run

# Real run: GPT agent, 4 clusters, reuse shared infra
python scripts/parallel/run_parallel.py \
    --agent gpt --workers 4 --problems all --reuse-infra --max-steps 30
```

`--problems` accepts:

| Form | Example | Meaning |
|------|---------|---------|
| `all` | `--problems all` | every problem in the registry |
| substring filter | `--problems detection` | all IDs containing `detection` |
| exact ID | `--problems pod_failure_hotel_res-detection-1` | one problem |
| comma list | `--problems a-id,b-id` | those IDs |
| file path | `--problems my_suite.txt` | one ID per line (`#` comments ok) |

Other flags: `--retries N` (default 1), `--timeout SECONDS` per problem (default
1800), `--keep-infra` (leave shared infra up after the run), `--list-problems`,
`--agent <name>` (any name in `clients/registry.py`).

Make sure API keys are set (`.env`) for the agent you choose.

### Scheduling: dynamic queue (default) vs static shards

By default (`--schedule queue`) all workers pull from **one shared queue** — any
cluster runs whatever problem is next when it goes idle. This maximizes
throughput: a cluster that draws fast problems just grabs more, so no worker sits
idle while another grinds through slow problems.

Use `--schedule static` to instead assign **fixed contiguous shards**, one per
cluster — e.g. with 50 problems and 2 clusters, `aiops-0` runs problems 1–25 and
`aiops-1` runs 26–50:

```bash
python scripts/parallel/run_parallel.py \
    --clusters aiops-0,aiops-1 --problems all --schedule static --dry-run
```

The `--dry-run` prints the exact per-cluster assignment so you can confirm it.
Extra problems when the count doesn't divide evenly go to the earliest clusters
(60 problems / 4 clusters → 15, 15, 15, 15; 58 → 15, 15, 14, 14).

| Mode | When to use |
|------|-------------|
| `queue` (default) | Fastest wall-clock; problem durations vary a lot. |
| `static` | Deterministic, reproducible cluster→problem mapping (debugging, pinning a noisy app to one cluster, isolating a subset per cluster). Note: a cluster whose shard holds the slow problems can finish later than the others. |

> Either mode works with `--reuse-infra`. In `static` mode, a cluster whose shard
> is empty (more clusters than problems) skips infra setup/teardown entirely.

---

## 3. Results

Everything lands under `data/results/parallel_<timestamp>/`:

```
parallel_20260616_120000/
├── config.json            # exact run configuration
├── summary.json           # aggregate + per-problem records
├── <session>_<t>.json     # full per-problem session traces (one per problem)
└── logs/<problem_id>.log  # full subprocess output per problem
```

The console prints a live tally and a final table (outcome · score · duration ·
cluster) plus the realized **speedup** (summed problem time ÷ wall clock).

`outcome` is derived from each task's metrics: `pass`/`fail` from
`Detection Accuracy`, `Localization Accuracy`, or `success`; `partial` for
non-exact localization; `timeout`/`error` for runs that didn't complete;
`unknown` for tasks with bespoke metrics (e.g. analysis) — the raw `results` are
always preserved in `summary.json`.

---

## How `--reuse-infra` works

The orchestrator gained two env-flag guards (default off, so normal/CLI runs are
unchanged):

- `AIOPSLAB_SKIP_INFRA_SETUP=true` — skip the per-problem OpenEBS + Prometheus install.
- `AIOPSLAB_SKIP_INFRA_TEARDOWN=true` — skip the per-problem uninstall.

With `--reuse-infra`, each worker runs `run_one.py --setup-infra` once on its
cluster, then runs every problem with both flags set, and finally
`run_one.py --teardown-infra` (unless `--keep-infra`). The shared bootstrap lives
in `Orchestrator.setup_cluster_infra()` / `teardown_cluster_infra()` — a single
source of truth shared by the normal path and the runner.

---

## Tuning & troubleshooting

| Symptom | Fix |
|---------|-----|
| OOM / clusters evicted | Fewer `--workers`; ~6–8 GB RAM each. |
| `Missing kubeconfig(s)` | Run `setup_clusters.py` first (the error prints the command). |
| A problem hangs | It's hard-killed at `--timeout` (whole process group) and retried; check `logs/<id>.log`. |
| Cluster left dirty after a kill | The next problem's `app.delete()` clears same-app residue; `--action reset` fully recreates a cluster. |
| Slow first problem per cluster | Expected without `--reuse-infra` (OpenEBS install). Use `--reuse-infra`. |
| Want max isolation, no reuse | Omit `--reuse-infra`; every problem installs/uninstalls its own infra. |

`--workers` defaults to `min(4, RAM / 8GB)`. Override explicitly for big hosts.
