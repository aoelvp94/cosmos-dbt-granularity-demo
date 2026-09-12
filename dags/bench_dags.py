"""The three execution shapes under benchmark, over the SAME generated dbt project.

1. ``bench_cosmos_per_model`` — Cosmos ``DbtDag`` in ``ExecutionMode.LOCAL``:
   one Airflow task per dbt model, each task a separate dbt invocation that
   re-parses the whole project. Retries are native and per-model (cheap) —
   this shape's win. Requires ``just manifest`` first (LoadMode.DBT_MANIFEST).

2. ``bench_batch_single`` — one BashOperator running ``dbt build`` over the
   whole project: one parse, dbt-internal threading. An Airflow retry re-runs
   EVERYTHING — this shape's loss.

3. ``bench_batch_retry`` — same single invocation, but retries run
   ``dbt retry`` against persisted state: only failed+skipped nodes re-run.

All are schedule=None; trigger via `just run-<shape>`.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import dag

from include.dbt_batch_retry import run_dbt_batch

PROJECT_DIR = "/usr/local/airflow/include/dbt_project"
MANIFEST = Path(PROJECT_DIR) / "target" / "manifest.json"
START = datetime(2026, 1, 1)
RETRY_ARGS = {"retries": 3, "retry_delay": timedelta(seconds=10)}


@dag(dag_id="bench_batch_single", schedule=None, start_date=START, catchup=False,
     max_active_runs=1, default_args=RETRY_ARGS, tags=["bench"])
def bench_batch_single():
    BashOperator(
        task_id="dbt_build_all",
        bash_command=(
            f"dbt build --project-dir {PROJECT_DIR} "
            f"--profiles-dir {PROJECT_DIR} --target bench"
        ),
    )


bench_batch_single()


@dag(dag_id="bench_batch_retry", schedule=None, start_date=START, catchup=False,
     max_active_runs=1, default_args=RETRY_ARGS, tags=["bench"])
def bench_batch_retry():
    PythonOperator(
        task_id="dbt_build_with_retry",
        python_callable=run_dbt_batch,
        op_kwargs={"project_dir": PROJECT_DIR},
        do_xcom_push=False,
    )


bench_batch_retry()


# Cosmos DAG only renders once a manifest exists (just manifest).
if MANIFEST.exists():
    from cosmos import (
        DbtDag,
        ExecutionConfig,
        ExecutionMode,
        InvocationMode,
        LoadMode,
        ProfileConfig,
        ProjectConfig,
        RenderConfig,
    )

    _PROJECT = ProjectConfig(dbt_project_path=PROJECT_DIR, manifest_path=str(MANIFEST))
    _PROFILE = ProfileConfig(
        profile_name="granularity_bench",
        target_name="bench",
        profiles_yml_filepath=f"{PROJECT_DIR}/profiles.yml",
    )
    _RENDER = RenderConfig(load_method=LoadMode.DBT_MANIFEST)

    bench_cosmos_per_model = DbtDag(
        dag_id="bench_cosmos_per_model",
        schedule=None,
        start_date=START,
        catchup=False,
        max_active_runs=1,
        max_active_tasks=8,  # match dbt threads=8 in the batch shapes
        default_args=RETRY_ARGS,
        tags=["bench"],
        project_config=_PROJECT,
        profile_config=_PROFILE,
        render_config=_RENDER,
        execution_config=ExecutionConfig(execution_mode=ExecutionMode.LOCAL),
        operator_args={"install_deps": False},
    )

    # Same per-model shape, but dbt invoked via the in-process dbtRunner
    # (skips CLI startup; the per-invocation parse remains).
    bench_cosmos_dbt_runner = DbtDag(
        dag_id="bench_cosmos_dbt_runner",
        schedule=None,
        start_date=START,
        catchup=False,
        max_active_runs=1,
        max_active_tasks=8,
        default_args=RETRY_ARGS,
        tags=["bench"],
        project_config=_PROJECT,
        profile_config=_PROFILE,
        render_config=_RENDER,
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.LOCAL, invocation_mode=InvocationMode.DBT_RUNNER
        ),
        operator_args={"install_deps": False},
    )

    # Cosmos's own facade mode: ONE producer task runs `dbt build` for the
    # whole selector; each model gets a lightweight consumer sensor that
    # mirrors its node status. Invocation cost O(1) + per-model UI states.
    bench_cosmos_watcher = DbtDag(
        dag_id="bench_cosmos_watcher",
        schedule=None,
        start_date=START,
        catchup=False,
        max_active_runs=1,
        default_args=RETRY_ARGS,
        tags=["bench"],
        project_config=_PROJECT,
        profile_config=_PROFILE,
        render_config=_RENDER,
        execution_config=ExecutionConfig(execution_mode=ExecutionMode.WATCHER),
        operator_args={"install_deps": False},
    )
