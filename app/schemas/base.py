from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class Metadata(BaseModel):
    fonte: str
    atualizado_em: str


class RespostaPadrao(BaseModel):
    ok: bool = True
    sistema: str
    indicador: str
    uf: Optional[str] = None
    municipio_ibge: Optional[str] = None
    ano: Optional[int] = None
    ano_inicio: Optional[int] = None
    ano_fim: Optional[int] = None
    total: int = 0
    dados: List[Dict[str, Any]] = Field(default_factory=list)
    metadata: Optional[Metadata] = None
