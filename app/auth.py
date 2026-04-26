from __future__ import annotations

from fastapi import Depends, HTTPException, status
from fastapi.security import APIKeyHeader

from .config import get_settings

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def require_api_key(api_key: str | None = Depends(api_key_header)) -> str:
    settings = get_settings()
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"ok": False, "erro": "X-API-Key header obrigatorio"},
        )
    if api_key not in settings.api_keys_list:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"ok": False, "erro": "API key invalida"},
        )
    return api_key
