from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, List, Dict

from .config import get_settings


def _build_uri(path: Path) -> str:
    # SQLite read-only URI mode (no writes, no WAL creation)
    p = path.resolve().as_posix()
    return f"file:{p}?mode=ro&immutable=0"


@contextmanager
def get_conn() -> Iterator[sqlite3.Connection]:
    settings = get_settings()
    uri = _build_uri(settings.sqlite_abs_path)
    conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def query(sql: str, params: tuple | dict = ()) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        cur = conn.execute(sql, params)
        rows = cur.fetchall()
        return [dict(r) for r in rows]


def query_one(sql: str, params: tuple | dict = ()) -> Dict[str, Any] | None:
    with get_conn() as conn:
        cur = conn.execute(sql, params)
        r = cur.fetchone()
        return dict(r) if r else None
