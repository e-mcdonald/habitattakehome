"""Database connection helpers.

Kept trivial — the whole app opens per-command connections. That's fine at
this scope; a pool would be the scale-up move.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psycopg


@contextmanager
def connect(database_url: str) -> Iterator[psycopg.Connection[Any]]:
    """Context manager yielding a Postgres connection.

    Commits nothing implicitly — callers are responsible for their own
    transaction boundaries via ``conn.commit()``.
    """
    with psycopg.connect(database_url) as conn:
        yield conn
