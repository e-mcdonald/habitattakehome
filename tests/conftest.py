"""Shared pytest fixtures.

Note the ``db`` marker: tests requiring a live Postgres are skipped unless
``DATABASE_URL`` is set in the environment. This lets ``pytest`` run cleanly
outside Docker while still exercising DB paths inside the compose container.
"""

from __future__ import annotations

import os
from pathlib import Path

import psycopg
import pytest


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Skip @pytest.mark.db tests when DATABASE_URL is unset."""
    skip_db = pytest.mark.skip(reason="DATABASE_URL not set — skipping DB-touching tests")
    if not os.environ.get("DATABASE_URL"):
        for item in items:
            if "db" in item.keywords:
                item.add_marker(skip_db)


@pytest.fixture(scope="session")
def project_root() -> Path:
    """Absolute path to the repo root."""
    return Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def fixture_records(project_root: Path) -> list[dict]:
    """The bundled 50-row sample used for offline tests + the offline demo."""
    import json

    path = project_root / "tests" / "fixtures" / "sample_records.json"
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture
def clean_db(project_root: Path) -> str:
    """Reset raw/staging/marts/ops to a known-empty state, then return DATABASE_URL.

    schema.sql itself is idempotent (``CREATE ... IF NOT EXISTS``) but does not
    reset data, so applying it alone isn't enough to isolate a test from rows
    left behind by a prior test run or a live pipeline run against the same
    database. Dropping the schemas first guarantees each @pytest.mark.db test
    starts from an empty database regardless of what ran before it.
    """
    database_url = os.environ["DATABASE_URL"]
    schema_sql = (project_root / "sql" / "schema.sql").read_text(encoding="utf-8")
    with psycopg.connect(database_url) as conn, conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS raw, staging, marts, ops CASCADE")
        cur.execute(schema_sql)
        conn.commit()
    return database_url
