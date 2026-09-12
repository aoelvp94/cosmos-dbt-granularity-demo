# Cosmos task-granularity benchmark. Run `just e2e` for the whole thing.
# Requires: astro CLI, Docker, python3, just.

set shell := ["bash", "-cu"]

n_models := "200"

# List recipes
default:
    @just --list

# Generate the synthetic dbt project (N models, 5 flaky-capable at the tail)
gen n=n_models:
    python3 scripts/generate_project.py {{n}}

# Build the image and start local Airflow (scheduler+api+postgres)
up:
    astro dev start --no-browser

down:
    astro dev stop

# Parse the project once inside the scheduler -> target/manifest.json (Cosmos needs it)
manifest:
    #!/usr/bin/env bash
    set -euo pipefail
    sch=$(docker ps -qf name=scheduler | head -1)
    [ -n "$sch" ] || { echo "run 'just up' first"; exit 1; }
    docker exec "$sch" bash -c 'cd /usr/local/airflow/include/dbt_project && dbt parse --profiles-dir . --target bench'
    docker exec "$sch" ls -la /usr/local/airflow/include/dbt_project/target/manifest.json

# Trigger one shape: cosmos | batch | retry
run shape:
    #!/usr/bin/env bash
    set -euo pipefail
    case "{{shape}}" in
      cosmos) dag=bench_cosmos_per_model ;;
      batch)  dag=bench_batch_single ;;
      retry)  dag=bench_batch_retry ;;
      *) echo "shape must be cosmos|batch|retry"; exit 1 ;;
    esac
    astro dev run dags unpause "$dag" >/dev/null || true
    astro dev run dags trigger "$dag"

# Make model <name> fail at runtime (e.g. just fail m_0199)
fail name:
    #!/usr/bin/env bash
    set -euo pipefail
    pg=$(docker ps -qf name=postgres | head -1)
    docker exec "$pg" psql -U postgres -c \
      "create schema if not exists bench_ctl; \
       create table if not exists bench_ctl.fail_flags (model_name text primary key, fail boolean not null default false); \
       insert into bench_ctl.fail_flags values ('{{name}}', true) \
       on conflict (model_name) do update set fail = true;"

# Clear all injected failures
heal:
    #!/usr/bin/env bash
    set -euo pipefail
    pg=$(docker ps -qf name=postgres | head -1)
    docker exec "$pg" psql -U postgres -c "update bench_ctl.fail_flags set fail = false;" || true

# Per-run metrics from the Airflow metadata DB: wall clock, task-seconds, retries
metrics:
    #!/usr/bin/env bash
    set -euo pipefail
    pg=$(docker ps -qf name=postgres | head -1)
    docker exec "$pg" psql -U postgres -x -c "select 1" >/dev/null
    docker exec "$pg" psql -U postgres -c "
      select ti.dag_id,
             substring(ti.run_id, 1, 26)                        as run_id,
             count(*)                                           as tasks,
             sum(ti.try_number - 1)                             as extra_tries,
             round(sum(ti.duration)::numeric, 1)                as task_seconds,
             round(extract(epoch from (max(ti.end_date) - min(ti.start_date)))::numeric, 1)
                                                                as wall_seconds,
             min(dr.state)                                      as run_state
      from task_instance ti
      join dag_run dr on dr.dag_id = ti.dag_id and dr.run_id = ti.run_id
      where ti.dag_id like 'bench_%'
      group by 1, 2
      order by min(ti.start_date) desc
      limit 15;"

# Full pipeline: generate, start, parse, run all three shapes
e2e n=n_models: (gen n) up manifest
    just run batch
    just run retry
    just run cosmos
    @echo "wait for runs to finish, then: just metrics"
