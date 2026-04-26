from __future__ import annotations

from typing import Optional
from pydantic import BaseModel


class SerieMensalRow(BaseModel):
    ano: int
    mes: int
    total_obitos: int


class MunicipioRow(BaseModel):
    municipio_ibge: str
    ano: Optional[int] = None
    total_obitos: int


class CausaRow(BaseModel):
    cid_3car: str
    descricao: Optional[str] = None
    ano: Optional[int] = None
    total_obitos: int


class CapituloRow(BaseModel):
    capitulo: str
    descricao: Optional[str] = None
    ano: Optional[int] = None
    total_obitos: int
    pct: Optional[float] = None


class PerfilRow(BaseModel):
    sexo: Optional[str] = None
    faixa_etaria: Optional[str] = None
    raca_cor: Optional[str] = None
    ano: Optional[int] = None
    total_obitos: int


class PartitionRow(BaseModel):
    uf: str
    ano: int
    linhas: int
