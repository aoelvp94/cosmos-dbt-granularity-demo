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

_(to be filled from `just metrics` — steady-state table and failure-recovery
table for N=200 and N=640)_

## Scope notes

- LocalExecutor: every shape runs subprocesses on the scheduler container, so
  the comparison isolates **invocation/parse overhead** — no pod-startup cost.
  On KubernetesExecutor each Cosmos task additionally pays pod startup and
  (in KUBERNETES mode) a second launcher pod; production numbers are larger.
- The retry-state directory is local (fine for LocalExecutor). On ephemeral
  workers, point the same ~150 lines at object storage.
