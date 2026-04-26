from __future__ import annotations

from fastapi import Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import HTTPException


async def http_exception_handler(request: Request, exc: HTTPException):
    detail = exc.detail
    if isinstance(detail, dict):
        body = {"ok": False, **detail} if "ok" not in detail else detail
    else:
        body = {"ok": False, "erro": str(detail)}
    return JSONResponse(status_code=exc.status_code, content=body)


async def generic_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"ok": False, "erro": "Erro interno do servidor", "detalhe": str(exc)},
    )
