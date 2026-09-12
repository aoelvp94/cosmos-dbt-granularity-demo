#!/usr/bin/env python3
"""Generate a synthetic dbt project with N models in a layered ref() graph.

Stdlib only — runs on the host. The project targets the astro-dev Postgres
(host ``postgres`` inside the compose network), so everything is local.

Layout produced under include/dbt_project/:
  dbt_project.yml, profiles.yml, models/gen/m_0001.sql ... m_NNNN.sql

Graph shape: the first ``roots`` models select literals; every later model
refs 1-2 earlier models (deterministic, seeded) so dbt has a real dependency
graph to order. All models are ``table`` materializations — each build does
actual (tiny) work in Postgres.

Flaky models: the last ``flaky`` models wrap their SELECT in a runtime check
against ``bench_ctl.fail_flags`` (created by an on-run-start hook). Setting
fail=true for a model makes its build divide by zero AT RUN TIME — the parse
hash never changes, mirroring a transient warehouse failure. Toggle with:
  just fail m_0199   /   just heal
"""

from __future__ import annotations

import argparse
import random
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROJECT = ROOT / "include" / "dbt_project"

DBT_PROJECT_YML = """\
name: granularity_bench
version: "1.0"
profile: granularity_bench
model-paths: ["models"]
target-path: target

on-run-start:
  - "create schema if not exists bench_ctl"
  - "create table if not exists bench_ctl.fail_flags (model_name text primary key, fail boolean not null default false)"

models:
  granularity_bench:
    +materialized: table
"""

PROFILES_YML = """\
granularity_bench:
  target: bench
  outputs:
    bench:
      type: postgres
      host: "{{ env_var('BENCH_PG_HOST', 'postgres') }}"
      port: 5432
      user: postgres
      password: postgres
      dbname: postgres
      schema: bench
      threads: {threads}
"""

PLAIN_SQL = """\
select
    {idx} as model_id,
    {refs_cols}
    now() as built_at
"""

FLAKY_SQL = """\
select
    {idx} as model_id,
    case
        when exists (select 1 from bench_ctl.fail_flags
                     where model_name = '{name}' and fail)
        then 1 / 0
        else 1
    end as flaky_check,
    {refs_cols}
    now() as built_at
"""


def gen(n: int, roots: int, flaky: int, threads: int, seed: int = 7) -> None:
    rng = random.Random(seed)
    models_dir = PROJECT / "models" / "gen"
    if PROJECT.exists():
        shutil.rmtree(PROJECT)
    models_dir.mkdir(parents=True)

    (PROJECT / "dbt_project.yml").write_text(DBT_PROJECT_YML)
    (PROJECT / "profiles.yml").write_text(PROFILES_YML.format(threads=threads))

    names = [f"m_{i:04d}" for i in range(1, n + 1)]
    for i, name in enumerate(names, start=1):
        if i <= roots:
            refs: list[str] = []
        else:
            k = 1 if rng.random() < 0.5 else 2
            refs = rng.sample(names[: i - 1], k=min(k, i - 1))
        refs_cols = "".join(
            f"(select count(*) from {{{{ ref('{r}') }}}}) as cnt_{r},\n    " for r in refs
        )
        template = FLAKY_SQL if i > n - flaky else PLAIN_SQL
        sql = template.format(idx=i, name=name, refs_cols=refs_cols)
        (models_dir / f"{name}.sql").write_text(sql)

    flaky_names = names[n - flaky:] if flaky else []
    print(f"generated {n} models ({roots} roots, flaky candidates: {', '.join(flaky_names) or 'none'})")
    print(f"project at {PROJECT}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("n", type=int, nargs="?", default=200, help="number of models")
    p.add_argument("--roots", type=int, default=10)
    p.add_argument("--flaky", type=int, default=5, help="how many tail models are flag-controlled")
    p.add_argument("--threads", type=int, default=8, help="dbt threads (match Airflow parallelism)")
    args = p.parse_args()
    gen(args.n, args.roots, args.flaky, args.threads)
