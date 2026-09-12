"""Batched dbt execution whose Airflow retries re-run ONLY failed+skipped nodes.

Reference implementation of the "batched execution mode" pattern:

* attempt 1 runs ``dbt build --select <selector>`` — ONE invocation, one parse;
* on failure, the attempt's ``target/run_results.json`` is persisted to a
  state directory keyed by (dag_id, task_id, run_id) and the task raises with
  the failed node names in the message;
* attempt N>1 finds that state and runs ``dbt retry --state <dir>`` — dbt
  replays the original invocation and re-runs only its failed+skipped nodes;
* success deletes the state, so clearing a green task falls back to a full
  build, while clearing a red task (try_number never resets) still replays
  only the failures.

State here is a local directory (fine for LocalExecutor, where retries land
on the same filesystem). On ephemeral workers (KubernetesExecutor) point the
same logic at object storage — the production version of this pattern
persists to S3.
"""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

STATE_ROOT = Path("/usr/local/airflow/include/.retry_state")
BAD_STATUSES = {"error", "fail", "skipped"}
MAX_NODES_IN_MESSAGE = 15


def _log_outcomes(results_path: Path) -> tuple[list[str], int]:
    try:
        results = json.loads(results_path.read_text())["results"]
    except (OSError, KeyError, ValueError):
        logger.warning("no parsable run_results.json at %s", results_path)
        return [], 0
    by_status: dict[str, int] = {}
    lines, bad = [], []
    for r in results:
        status = r.get("status", "?")
        by_status[status] = by_status.get(status, 0) + 1
        took = r.get("execution_time")
        took_s = f"{took:7.2f}s" if isinstance(took, (int, float)) else "       -"
        lines.append(f"  {status:8s} {took_s}  {r.get('unique_id', '?')}")
        if status in BAD_STATUSES:
            message = (r.get("message") or "").replace("\n", " ")[:160]
            bad.append(f"{r.get('unique_id')} [{status}] {message}".rstrip())
    counts = ", ".join(f"{k}={v}" for k, v in sorted(by_status.items()))
    logger.info("dbt batch outcome (%d nodes): %s\n%s", len(results), counts, "\n".join(lines))
    return bad, len(results)


def run_dbt_batch(
    *,
    project_dir: str,
    select: str | None = None,
    exclude: str | None = None,
    target: str = "bench",
    **context: Any,
) -> None:
    ti = context["ti"]
    state_dir = STATE_ROOT / ti.dag_id / ti.task_id / context["run_id"]
    state_file = state_dir / "run_results.json"

    base = f"--project-dir {project_dir} --profiles-dir {project_dir} --target {target}"
    if ti.try_number > 1 and state_file.exists():
        prev = json.loads(state_file.read_text())["results"]
        failed = [r["unique_id"] for r in prev if r.get("status") in ("error", "fail")]
        skipped = [r["unique_id"] for r in prev if r.get("status") == "skipped"]
        logger.info(
            "RETRY MODE: previous attempt had %d failed + %d skipped of %d nodes; "
            "dbt retry re-runs only those. Failed: %s",
            len(failed), len(skipped), len(prev), ", ".join(failed[:MAX_NODES_IN_MESSAGE]),
        )
        cmd = f"dbt retry --state {state_dir} {base}"
    else:
        select_arg = f"--select '{select}' " if select else ""
        exclude_arg = f"--exclude '{exclude}' " if exclude else ""
        cmd = f"dbt build {select_arg}{exclude_arg}{base}"

    logger.info("Running: %s", cmd)
    proc = subprocess.run(["bash", "-c", cmd], check=False)

    results_path = Path(project_dir) / "target" / "run_results.json"
    bad, total = _log_outcomes(results_path)

    if proc.returncode == 0 and not bad:
        shutil.rmtree(state_dir, ignore_errors=True)
        return

    if results_path.exists():
        state_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy(results_path, state_file)
        logger.info("persisted retry state to %s — next attempt runs `dbt retry`", state_file)

    shown = "; ".join(bad[:MAX_NODES_IN_MESSAGE])
    more = f" (+{len(bad) - MAX_NODES_IN_MESSAGE} more)" if len(bad) > MAX_NODES_IN_MESSAGE else ""
    raise RuntimeError(
        f"dbt batch failed: {len(bad)}/{total} nodes not successful — {shown}{more}"
        if bad
        else f"dbt exited rc={proc.returncode} before producing node results"
    )
