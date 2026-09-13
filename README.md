# Cosmos dbt task granularity benchmark

[astronomer-cosmos](https://github.com/astronomer/astronomer-cosmos) couples
**task granularity to invocation granularity**: one Airflow task per dbt model
= one full project parse per model. The common escape (one big `dbt build`
task) fixes the parse cost but makes every Airflow retry re-run the whole
selector. This repo measures both, plus the missing third shape: **a batched
task whose retries run `dbt retry`** — re-running only failed + skipped nodes.

Fully local: `docker compose` (Airflow 3 + Postgres), synthetic N-model dbt
project, no cloud. `just e2e` reproduces everything.

## Results (N=200 models, parallelism 8 everywhere)

**Steady state** — build all models once:

| shape | tasks | dbt invocations | task-seconds | wall |
|---|---:|---:|---:|---:|
| single batch (`dbt build`) | 1 | 1 | 7.7 | 7.7 s |
| **batch + dbt retry** (this repo) | 1 | 1 | **7.4** | **7.4 s** |
| Cosmos `LOCAL` per-model | 200 | 200 | 577.9 | 85.0 s |
| Cosmos `LOCAL` + `DBT_RUNNER` | 200 | 200 | 539.7 | 82.5 s |
| Cosmos `WATCHER` | 201 | 1 | 185.6 | 38.4 s |

**Recovery** — k models fail transiently, heal after attempt 1:

| shape | recovery cost (k=1) | recovery cost (k=5) | what re-runs |
|---|---:|---:|---|
| single batch | 8.5 s | 7.3 s | all N models |
| **batch + dbt retry** | **2.9 s** | **2.0 s** | only the k failed |
| Cosmos per-model | 3.0 s | ~3 s/node | only failed tasks |
| Cosmos `WATCHER` | ~1.7–3.4 s/node | — | failed node, standalone invocation |

**The matrix:**

|  | steady state | recovery | per-model UI |
|---|---|---|---|
| Cosmos per-model | O(N) invocations ✗ | O(k) ✓ | ✓ |
| Cosmos WATCHER | O(1) invocations, O(N) consumer tasks (24× batch) | O(k) ✓ | ✓ |
| single batch | O(1) ✓ | O(N) ✗ | ✗ |
| **batch + dbt retry** | **O(1) ✓** | **O(k) ✓** | logs only |

## Takeaways

1. **Per-model overhead is ~2.9 s/task** (import + Airflow + parse + connect)
   for models that build in milliseconds — 75× the compute of one batch.
   `InvocationMode.DBT_RUNNER` shaves only ~7% (CLI startup ≈ 0.2 s): parse
   dominates, so cheaper invocations don't fix it. And it compounds
   quadratically: each of N tasks parses an N-model project.
2. **`ExecutionMode.WATCHER` (1.15, experimental) is most of the answer**:
   one producer invocation + per-model UI, and clever recovery (producer
   skips itself on retry; the failed node re-runs standalone). What remains:
   O(N) consumer *tasks* (24× batch compute here; a pod each on ephemeral-
   worker executors), a 2.5×-slower producer, XCom-based state.
3. **`dbt retry` closes the batch's only gap** for free: identical steady
   state, O(k) recovery with a single parse for all k failures, failed-model
   names in the exception. Operator: [`include/dbt_batch_retry.py`](include/dbt_batch_retry.py) (~150 lines,
   attempt state persisted per (dag, task, run); production version uses S3).
4. **Upstream ask, sharpened**: per-node *results* without per-node *tasks*
   (or free consumers), plus `dbt retry` in WATCHER's producer.

## Run it

```bash
just e2e            # generate 200 models, start Airflow, run the shapes
just metrics        # wall / task-seconds / retries from the metadata DB
just fail m_0199    # runtime-fail one model (parse hash untouched); just heal
just gen 1000       # rerun at another scale
```

Details, caveats, per-attempt tables and environment notes:
[docs/NOTES.md](docs/NOTES.md).
