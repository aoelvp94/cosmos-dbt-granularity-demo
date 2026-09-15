"""``dbt build`` as ONE Cosmos task whose Airflow retries run ``dbt retry``.

Operator form of the pattern measured by ``include/dbt_batch_retry.py``, written
to Cosmos's own conventions so it can be proposed upstream roughly as-is
(candidate home: ``cosmos/operators/retry.py``). It subclasses
``DbtBuildLocalOperator``, so it inherits the whole local-execution path —
project cloning, partial-parse cache, profile rendering, OpenLineage, datasets,
``on_kill`` — and changes exactly one thing: what a *second* attempt runs.

Shape
-----
Attempt 1 runs the normal Cosmos ``dbt build``. If any node ends in a retryable
status, that attempt's ``run_results.json`` is copied out of the temporary
project directory into a state store keyed by
``(dag_id, task_id, map_index, run_id)``. Persisting it is what makes the replay
possible at all: ``run_command`` builds inside a fresh
``tempfile.TemporaryDirectory`` on every attempt, so the artifact is gone by the
time the next one starts. Attempt N>1 finds that state and swaps ``build`` for
``dbt retry --state <dir>`` — dbt replays the previous invocation and re-runs
only its failed + skipped nodes, in ONE parse. Success clears the state, so
clearing a green task falls back to a full build while clearing a red one
(``try_number`` never resets) still replays only the failures.

The point is to hold BOTH ends: O(1) dbt invocations in the steady state (one
parse for N models) and O(k) recovery (one parse for k failures), without the
O(N) Airflow tasks that buy that today.

What ``dbt retry`` actually accepts
-----------------------------------
``dbt retry`` is not ``dbt build``. Its click command declares the global flags
plus ``--project-dir``, ``--profiles-dir``, ``--vars``, ``--target-path``,
``--threads`` and ``--full-refresh`` — and nothing else. The node-selection
flags Cosmos normally emits (``--select/--exclude/--selector/--models``) are not
options of it and make click abort, so they are stripped on a replay.
``--profile``, ``--target``, ``--state``, ``--indirect-selection`` and
``--no-static-parser`` ARE global flags, so everything Cosmos's own
``_generate_dbt_flags`` emits passes through untouched.

dbt's ``RetryTask`` lists ``project_dir`` and ``profiles_dir`` in its
``IGNORE_PARENT_FLAGS``: it takes them from the *current* invocation rather than
from the recorded ones. That is precisely what makes this work under Cosmos,
whose project directory is a different temp path on every attempt — no rewriting
of the persisted artifact is needed.

State location
--------------
``[cosmos] remote_target_path`` when one is configured, the local filesystem
otherwise. The local store is only correct when retries land on the same
filesystem (LocalExecutor, or a shared volume); on ephemeral workers
(KubernetesExecutor) configure ``remote_target_path`` or pass ``state_path``.
"""

from __future__ import annotations

import json
import shutil
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from airflow.exceptions import AirflowException

from cosmos.airflow.compatibility import AirflowSkipException
from cosmos.log import get_logger
from cosmos.operators.local import DbtBuildLocalOperator
from cosmos.settings import cache_dir, remote_target_path, remote_target_path_conn_id

try:
    from airflow.sdk import ObjectStoragePath  # Airflow 3
except ImportError:  # pragma: no cover
    from airflow.io.path import ObjectStoragePath  # Airflow 2

if TYPE_CHECKING:  # pragma: no cover
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        from airflow.utils.context import Context  # type: ignore[attr-defined]

logger = get_logger(__name__)

RUN_RESULTS_FILE_NAME = "run_results.json"

# Mirrors dbt.task.retry.RETRYABLE_STATUSES without importing dbt internals into
# the Airflow process: these are the statuses `dbt retry` will pick back up.
RETRYABLE_STATUSES = frozenset({"error", "fail", "skipped", "runtime error", "partial success"})

# Node selection is replayed from run_results.json, and these are not options of
# the `retry` click command — passing them makes dbt abort with a usage error.
SELECTION_FLAGS = ("--select", "--exclude", "--selector", "--models")

DEFAULT_LOCAL_STATE_ROOT = cache_dir / "retry_state"
REMOTE_STATE_SUBDIR = "_cosmos_retry_state"
MAX_NODES_IN_MESSAGE = 15


def _drop_flag_pairs(flags: list[str], unwanted: Sequence[str]) -> list[str]:
    """Remove ``--flag value`` pairs from an assembled dbt command.

    Cosmos emits every global flag as two list entries (``_process_global_flag``
    returns ``[name, value]``), so dropping a flag means dropping its value too.
    """
    kept: list[str] = []
    skip_value = False
    for flag in flags:
        if skip_value:
            skip_value = False
            continue
        if flag in unwanted:
            skip_value = True
            continue
        kept.append(flag)
    return kept


def _retryable_nodes(run_results: dict[str, Any]) -> list[str]:
    return [
        result.get("unique_id", "?")
        for result in run_results.get("results", [])
        if result.get("status") in RETRYABLE_STATUSES
    ]


def _status_counts(run_results: dict[str, Any]) -> str:
    counts: dict[str, int] = {}
    for result in run_results.get("results", []):
        status = result.get("status", "?")
        counts[status] = counts.get(status, 0) + 1
    return ", ".join(f"{status}={count}" for status, count in sorted(counts.items())) or "no nodes"


def _sanitize(value: str) -> str:
    return value.replace("/", "_").replace("\\", "_")


class _RunResultsStore:
    """Where a failed attempt's ``run_results.json`` waits for the next attempt.

    Backed by a ``pathlib.Path`` or an ``ObjectStoragePath``; the two share
    enough of an interface (``/``, ``exists``, ``mkdir``, ``open``) that only
    deletion needs to branch.
    """

    def __init__(self, base: Path | ObjectStoragePath) -> None:
        self._base = base
        self._is_local = isinstance(base, Path)

    def __str__(self) -> str:
        return str(self._base)

    def _dir(self, key: str) -> Any:
        return self._base / key

    def _file(self, key: str) -> Any:
        return self._dir(key) / RUN_RESULTS_FILE_NAME

    def read(self, key: str) -> dict[str, Any] | None:
        path = self._file(key)
        try:
            if not path.exists():
                return None
            with path.open("r") as fp:
                return json.load(fp)  # type: ignore[no-any-return]
        except (OSError, ValueError) as exc:
            logger.warning("Ignoring unreadable retry state at %s: %s", path, exc)
            return None

    def write(self, key: str, payload: dict[str, Any]) -> str:
        path = self._file(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as fp:
            json.dump(payload, fp)
        return str(path)

    def delete(self, key: str) -> None:
        try:
            if self._is_local:
                shutil.rmtree(self._dir(key), ignore_errors=True)
                return
            path = self._file(key)
            if path.exists():
                path.unlink()
        except OSError as exc:
            logger.warning("Could not clear retry state for %s: %s", key, exc)


class DbtBuildRetryLocalOperator(DbtBuildLocalOperator):
    """Run ``dbt build`` once; re-run only what failed on every Airflow retry.

    :param state_path: Directory or URI holding per-attempt ``run_results.json``.
        Defaults to ``[cosmos] remote_target_path`` when set, otherwise to a
        subdirectory of Cosmos's ``cache_dir`` on the local filesystem.
    :param state_conn_id: Airflow connection for ``state_path`` when it is
        remote. Defaults to ``[cosmos] remote_target_path_conn_id``.
    """

    template_fields: Sequence[str] = tuple(DbtBuildLocalOperator.template_fields) + ("state_path",)

    # Class-level defaults so ``base_cmd`` is answerable before ``__init__``
    # finishes and between executions.
    _replay_state_dir: Path | None = None
    _run_results: dict[str, Any] | None = None

    def __init__(
        self,
        *args: Any,
        state_path: str | None = None,
        state_conn_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        self.state_path = state_path
        self.state_conn_id = state_conn_id
        super().__init__(*args, **kwargs)

    @property
    def _replaying(self) -> bool:
        return self._replay_state_dir is not None

    @property
    def base_cmd(self) -> list[str]:
        """``build`` on a first attempt, ``retry`` once there is state to replay."""
        return ["retry"] if self._replaying else ["build"]

    def add_global_flags(self) -> list[str]:
        flags = super().add_global_flags()
        if not self._replaying:
            return flags
        return _drop_flag_pairs(flags, SELECTION_FLAGS)

    def add_cmd_flags(self) -> list[str]:
        if not self._replaying:
            return super().add_cmd_flags()
        # dbt reads --full-refresh and every other build-time flag back out of
        # the recorded args; the replay only needs to be pointed at them.
        return ["--state", str(self._replay_state_dir)]

    def _handle_post_execution(
        self, tmp_project_dir: str, context: Context, push_run_results_to_xcom: bool = False
    ) -> None:
        # Called from inside run_command's TemporaryDirectory and before
        # handle_exception — the last moment at which this attempt's
        # run_results.json still exists on disk.
        self._run_results = self._load_run_results(Path(tmp_project_dir) / "target" / RUN_RESULTS_FILE_NAME)
        super()._handle_post_execution(tmp_project_dir, context, push_run_results_to_xcom)

    def execute(self, context: Context, **kwargs: Any) -> None:
        store = self._build_store()
        key = self._state_key(context)
        self._run_results = None
        replay_root: str | None = None

        previous = store.read(key)
        try:
            if previous is not None:
                replay_root = tempfile.mkdtemp(prefix="cosmos-dbt-retry-")
                self._replay_state_dir = Path(replay_root)
                (self._replay_state_dir / RUN_RESULTS_FILE_NAME).write_text(json.dumps(previous))
                self._log_replay_plan(previous)
            super().execute(context, **kwargs)
        except AirflowSkipException:
            # skip_exit_code: not a failure, and nothing to replay.
            store.delete(key)
            raise
        except Exception as exc:
            failed = self._persist_failure(store, key)
            if not failed:
                raise
            shown = ", ".join(failed[:MAX_NODES_IN_MESSAGE])
            more = f" (+{len(failed) - MAX_NODES_IN_MESSAGE} more)" if len(failed) > MAX_NODES_IN_MESSAGE else ""
            raise AirflowException(f"{exc} — {len(failed)} node(s) not successful: {shown}{more}") from exc
        else:
            store.delete(key)
        finally:
            self._replay_state_dir = None
            if replay_root:
                shutil.rmtree(replay_root, ignore_errors=True)

    def _build_store(self) -> _RunResultsStore:
        base = self.state_path
        if base is None and remote_target_path:
            base = f"{str(remote_target_path).rstrip('/')}/{REMOTE_STATE_SUBDIR}"
        if base is None:
            return _RunResultsStore(DEFAULT_LOCAL_STATE_ROOT)

        scheme = urlparse(str(base)).scheme
        if scheme in ("", "file"):
            return _RunResultsStore(Path(str(base).removeprefix("file://")))

        conn_id = self.state_conn_id or remote_target_path_conn_id
        return _RunResultsStore(ObjectStoragePath(str(base), conn_id=conn_id))

    @staticmethod
    def _state_key(context: Context) -> str:
        ti = context["ti"]
        parts = [ti.dag_id, ti.task_id, str(context["run_id"])]
        map_index = getattr(ti, "map_index", -1)
        if map_index is not None and map_index >= 0:
            parts.append(f"map_{map_index}")
        return "/".join(_sanitize(part) for part in parts)

    def _load_run_results(self, path: Path) -> dict[str, Any] | None:
        try:
            return json.loads(path.read_text())  # type: ignore[no-any-return]
        except (OSError, ValueError) as exc:
            self.log.warning("No parsable %s at %s: %s", RUN_RESULTS_FILE_NAME, path, exc)
            return None

    def _log_replay_plan(self, previous: dict[str, Any]) -> None:
        failed = _retryable_nodes(previous)
        self.log.info(
            "RETRY MODE: previous attempt recorded %d node(s) (%s); `dbt retry` re-runs the %d retryable one(s) "
            "in a single invocation: %s",
            len(previous.get("results", [])),
            _status_counts(previous),
            len(failed),
            ", ".join(failed[:MAX_NODES_IN_MESSAGE]) or "none",
        )

    def _persist_failure(self, store: _RunResultsStore, key: str) -> list[str]:
        if self._run_results is None:
            # dbt died before producing node results (bad profile, parse error).
            # Nothing to replay — drop any stale state so the next attempt is a
            # clean full build.
            self.log.warning("No %s from this attempt; the next one runs a full build.", RUN_RESULTS_FILE_NAME)
            store.delete(key)
            return []

        failed = _retryable_nodes(self._run_results)
        if not failed:
            store.delete(key)
            return []

        location = store.write(key, self._run_results)
        self.log.info(
            "Persisted %d retryable node(s) to %s — the next attempt runs `dbt retry` against it.",
            len(failed),
            location,
        )
        return failed
