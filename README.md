# Cosmos dbt task granularity: one parse per model vs one per batch

A self-contained, fully local benchmark of a structural cost in
[astronomer-cosmos](https://github.com/astronomer/astronomer-cosmos):
**task granularity is coupled to invocation granularity**. One Airflow task
per dbt model means one full dbt invocation — and one full *project parse* —
per model. On a project with hundreds of models, parse overhead dominates
actual build time.

The usual escape hatch (run the whole selector as ONE `dbt build` task) fixes
the parse cost but gives up per-model retry: an Airflow retry re-runs the
entire batch. This repo benchmarks both shapes **plus the missing third one**
— a batched task whose retries run `dbt retry`, re-running only the failed +
skipped nodes of the previous attempt.

```
cosmos per-model            single batch               batch + dbt retry
(N tasks, N parses)         (1 task, 1 parse)          (1 task, 1 parse)

retry of 1 failure:         retry of 1 failure:        retry of 1 failure:
  re-run 1 model  ✓           re-run ALL N models ✗      dbt retry -> 1 model ✓
steady-state cost:          steady-state cost:         steady-state cost:
  N x (parse + model) ✗       1 parse + N models ✓       1 parse + N models ✓
```

Everything runs on your machine: `astro dev` (Astronomer's local Airflow 3)
with dbt targeting the bundled Postgres. **No warehouse, no cloud, nothing
uploaded anywhere.**

## Why this matters

Measured on a production project (~640 models, Athena): each Cosmos
`ExecutionMode.LOCAL` task pays ~30 s / ~630 MiB / ~1 CPU-core-burst just to
parse the project before building one model. At a 10-minute cadence that
parse tax dominates the bill — enough that the scheduled pipeline abandoned
Cosmos for hand-rolled batched `dbt build` tasks, and then lived with the
retry regression this repo's third shape fixes.

## The three shapes

| DAG | execution | retry semantics |
|---|---|---|
| `bench_cosmos_per_model` | Cosmos `DbtDag`, `ExecutionMode.LOCAL`, one task per model | native per-model (its win) |
| `bench_batch_single` | one `BashOperator` running `dbt build` | re-runs everything (its loss) |
| `bench_batch_retry` | one task + [`include/dbt_batch_retry.py`](include/dbt_batch_retry.py) | `dbt retry` — failed+skipped only |

The retry operator persists each failed attempt's `run_results.json` keyed by
(dag, task, run); attempt N>1 replays it with `dbt retry`. Success deletes
the state (clearing a green task full-builds; clearing a red task — Airflow
never resets `try_number` — replays only the failures).

## Run it

```bash
just e2e            # generate 200 models, start Airflow, parse, trigger all three
just metrics        # per-run wall clock / task-seconds / retries from the metadata DB
```

Failure-injection scenario (the retry benchmark):

```bash
just fail m_0199    # runtime-fails one model (parse hash untouched)
just run batch      # -> retry re-runs all 200
just run retry      # -> retry re-runs 1
just run cosmos     # -> retry re-runs 1 (native)
just heal
```

Knobs: `just gen 640` regenerates at another size; dbt `threads` and Cosmos
`max_active_tasks` are both 8 so wall-clock comparisons are fair.

## Results

Measured 2026-09-12 on an M-series MacBook Pro, N=200 models, dbt `threads: 8`
= Cosmos `max_active_tasks: 8`, Airflow 3.3 (LocalExecutor) + dbt-core 1.12.4
+ astronomer-cosmos 1.15.1, Postgres 16 in the same compose network. All runs
`success`; durations from the Airflow metadata DB (`task_instance`,
`task_instance_history`).

**Steady state** (build all 200 models once):

| shape | Airflow tasks | dbt invocations | task-seconds | wall-clock |
|---|---:|---:|---:|---:|
| single batch (`dbt build`) | 1 | 1 | **7.7 s** | **7.7 s** |
| batch + dbt retry (this repo's operator) | 1 | 1 | **7.4 s** | **7.4 s** |
| Cosmos per-model (`ExecutionMode.LOCAL`) | 200 | 200 | **577.9 s** | **85.0 s** |

Identical work, **~75× the compute and ~11× the wall-clock** for per-model
tasks — pure invocation overhead (~2.9 s/task to import Airflow + dbt, parse,
connect, build one trivial model). This synthetic project parses in ~1 s; on
the production project that motivated this repo a parse is ~30 s, which is why
the ratio there was pipeline-abandoning rather than merely ugly. The retry
operator's steady-state cost is indistinguishable from the plain batch — the
retry machinery is free until a failure happens.

**Recovery** (make `m_0199` fail at runtime, heal the flag after attempt 1,
let `retries` recover the run — per-attempt durations):

| shape | attempt 1 | attempt 2 (recovery) | what attempt 2 re-ran |
|---|---:|---:|---|
| single batch | 8.0 s (failed) | **8.5 s** | **all 200 models** |
| batch + dbt retry | 8.5 s (failed) | **2.9 s** | **only `m_0199`** |
| Cosmos per-model | 2.9 s (failed task) | **3.0 s** | only `m_0199`'s task |

The operator's attempt-2 log, verbatim:

```
RETRY MODE: previous attempt had 1 failed + 0 skipped of 202 nodes;
dbt retry re-runs only those. Failed: model.granularity_bench.m_0199
```

**Recovery at k=5** (all five flaky models failing at once, healed after the
first failure lands):

| shape | attempt 1 | attempt 2 (recovery) | what attempt 2 re-ran | run wall-clock |
|---|---:|---:|---|---:|
| single batch | 8.8 s (5 failures) | **7.3 s** | **all 200 models** | 26.9 s |
| batch + dbt retry | 7.7 s (5 failures) | **2.0 s** | **only the 5 failed** | **20.2 s** |
| Cosmos per-model | 2.8 s (failed task) | 3.1 s | 1 task | 84.1 s |

Operator log, verbatim: `RETRY MODE: previous attempt had 5 failed + 0
skipped of 202 nodes; dbt retry re-runs only those. Failed: …m_0196, m_0197,
m_0200, m_0199, m_0198`.

Honest caveat on the Cosmos row: its 200 tasks spread over ~85 s, so four of
the five flaky models ran *after* the heal and passed on try 1 — only one
task actually retried. That temporal dispersion dodging short transients is
real per-task behavior, but it means this row is effectively k=1; a strict
k=5 for Cosmos requires holding the flag until all five have failed. Its
recovery is O(k) by construction either way — its problem is the steady-state
column.

At toy scale 8.5 s vs 2.9 s looks mild; the point is the asymptotics — plain
batch recovery is O(whole selector), `dbt retry` recovery is O(failures) and
independent of k while k ≪ N, matching Cosmos's per-model retry granularity
while keeping the batch's steady-state cost. That's the full two-axis picture:

|  | steady state | recovery from k failures |
|---|---|---|
| Cosmos per-model | O(N) invocations ✗ | O(k) ✓ |
| single batch | O(1) invocation ✓ | O(N) ✗ |
| **batch + dbt retry** | **O(1) ✓** | **O(k) ✓** |

## Two more Cosmos shapes: InvocationMode.DBT_RUNNER and ExecutionMode.WATCHER

Cosmos 1.15 offers two mitigations worth measuring against the same project:

| shape | tasks | task-seconds | wall-clock |
|---|---:|---:|---:|
| single batch | 1 | 7.7 s | 7.7 s |
| batch + dbt retry | 1 | 7.4 s | 7.4 s |
| Cosmos LOCAL, subprocess | 200 | 577.9 s | 85.0 s |
| Cosmos LOCAL, `InvocationMode.DBT_RUNNER` | 200 | 539.7 s | 82.5 s |
| Cosmos `ExecutionMode.WATCHER` | 201 | 185.6 s | 38.4 s |

**DBT_RUNNER shaves only ~7%.** CLI startup is ~0.2 s of the ~2.9 s per-task
overhead — the rest is import + parse. Making invocations cheaper doesn't fix
per-model execution; only not doing N invocations does.

**WATCHER is Cosmos's own facade mode and it largely works** (Airflow 3 +
Postgres, experimental): ONE producer task runs `dbt build` for the whole
selector, and each model gets a consumer task that mirrors its node status —
per-model UI states at O(1) dbt invocations. Its costs here: the producer ran
the same build in 19.1 s (~2.5× the bare batch — event-streaming overhead)
and the 200 consumers averaged 0.83 s each, totalling 185.6 task-seconds —
**24× the compute and 5× the wall-clock of the plain batch**, all task/
scheduler churn rather than dbt work. On an ephemeral-worker executor
(KubernetesExecutor) each consumer becomes a pod, which re-imports most of
the per-model overhead this mode exists to avoid.

**WATCHER's recovery semantics (measured, and genuinely clever):** with one
model failing transiently, attempt 1's producer fails after the full build
(19.9 s — unavoidable in any batched shape); on retry the producer *skips
itself* and only the failed node's task re-runs that single model as a
standalone invocation (3.4 s while still failing, 1.7 s once healed). So
WATCHER is O(k) on recovery too — the recovering node pays one full
per-node invocation (parse included), where `dbt retry` pays one parse for
all k failures together.

Updated matrix:

|  | steady state | recovery | per-model UI |
|---|---|---|---|
| Cosmos per-model | O(N) invocations ✗ | O(k) ✓ | ✓ |
| Cosmos WATCHER | O(1) invocations, +O(N) consumer tasks (≈24× batch here) | O(k), one invocation per failed node ✓ | ✓ |
| single batch | O(1) ✓ | O(N) ✗ | ✗ |
| batch + dbt retry | **O(1) ✓** | **O(k), one invocation total ✓** | ✗ (logs only) |

So the sharpened upstream framing: WATCHER already delivers per-model
visibility and node-level recovery over a single invocation — what keeps it
from replacing hand-rolled batches at scale is the O(N) consumer-task churn
(fatal on ephemeral-worker executors) and the producer's streaming overhead.
A batched mode that reports per-node results *without* materializing a task
per node — or a WATCHER whose consumers are free — plus `dbt retry` in the
producer would close the gap entirely.

## Scope notes

- LocalExecutor: every shape runs subprocesses on the scheduler container, so
  the comparison isolates **invocation/parse overhead** — no pod-startup cost.
  On KubernetesExecutor each Cosmos task additionally pays pod startup and
  (in KUBERNETES mode) a second launcher pod; production numbers are larger.
- The retry-state directory is local (fine for LocalExecutor). On ephemeral
  workers, point the same ~150 lines at object storage.
