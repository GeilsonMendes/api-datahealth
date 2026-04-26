from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter

from ..config import get_settings
from ..db import get_conn

router = APIRouter(tags=["system"])


@router.get("/health")
def health():
    return {"ok": True, "servico": "api-datahealth", "ts": datetime.now(timezone.utc).isoformat()}


@router.get("/datasus/status")
def datasus_status():
    settings = get_settings()
    db_path: Path = settings.sqlite_abs_path
    info = {
        "ok": True,
        "sqlite_path": str(db_path),
        "exists": db_path.exists(),
        "size_bytes": db_path.stat().st_size if db_path.exists() else 0,
    }
    if db_path.exists():
        try:
            with get_conn() as conn:
                tables = [
                    r[0]
                    for r in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'insights_sim_%' ORDER BY name"
                    ).fetchall()
                ]
                info["tabelas_sim"] = tables
        except sqlite3.Error as e:
            info["erro"] = str(e)
    return info
