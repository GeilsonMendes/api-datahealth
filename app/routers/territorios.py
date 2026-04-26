from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query

from ..auth import require_api_key
from ..db import query

router = APIRouter(
    tags=["Territorios e Populacao"],
    dependencies=[Depends(require_api_key)],
)

FONTE = "IBGE"


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _envelope(indicador: str, dados: List[Dict[str, Any]], **filtros: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "ok": True,
        "sistema": "territorios",
        "indicador": indicador,
    }
    for k, v in filtros.items():
        if v is not None:
            out[k] = v
    out["total"] = len(dados)
    out["dados"] = dados
    out["metadata"] = {"fonte": FONTE, "atualizado_em": _now_iso()}
    return out


# ---------------------------------------------------------------------------
# /territorios
# ---------------------------------------------------------------------------

@router.get("/territorios", summary="Listar territorios (estados, municipios, regioes)")
def listar_territorios(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    tipo: str = Query("municipio", pattern="^(estado|municipio|regiao)$"),
    q: Optional[str] = Query(None, description="Busca por nome (LIKE %q%)"),
    limit: int = Query(100, ge=1, le=1000),
):
    clauses: List[str] = ["tipo = ?"]
    params: List[Any] = [tipo]
    if uf:
        clauses.append("uf = ?")
        params.append(uf.upper())
    if q:
        clauses.append("nome LIKE ?")
        params.append(f"%{q}%")
    where = "WHERE " + " AND ".join(clauses)
    sql = (
        f"SELECT id, codigo_ibge, nome, tipo, uf, pai_id "
        f"FROM territorios {where} ORDER BY nome LIMIT ?"
    )
    rows = query(sql, tuple(params) + (limit,))
    return _envelope("territorios", rows, uf=uf.upper() if uf else None, tipo=tipo, q=q)


# ---------------------------------------------------------------------------
# /populacao/municipio
# ---------------------------------------------------------------------------

@router.get("/populacao/municipio", summary="Populacao por municipio (IBGE)")
def populacao_municipio(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
    ano: Optional[int] = Query(None, ge=1990, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=1990, le=2030),
    ano_fim: Optional[int] = Query(None, ge=1990, le=2030),
):
    clauses: List[str] = []
    params: List[Any] = []
    if uf:
        clauses.append("uf = ?")
        params.append(uf.upper())
    if municipio_ibge:
        clauses.append("municipio_ibge = ?")
        params.append(municipio_ibge)
    if ano is not None:
        clauses.append("ano = ?")
        params.append(ano)
    else:
        if ano_inicio is not None:
            clauses.append("ano >= ?")
            params.append(ano_inicio)
        if ano_fim is not None:
            clauses.append("ano <= ?")
            params.append(ano_fim)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    sql = (
        f"SELECT municipio_ibge, municipio_nome, uf, ano, populacao "
        f"FROM populacao_municipio {where} "
        f"ORDER BY uf, municipio_nome, ano"
    )
    rows = query(sql, tuple(params))
    return _envelope(
        "populacao.municipio", rows,
        uf=uf.upper() if uf else None,
        municipio_ibge=municipio_ibge,
        ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )


# ---------------------------------------------------------------------------
# /populacao/uf
# ---------------------------------------------------------------------------

@router.get("/populacao/uf", summary="Projecao de populacao por UF (IBGE)")
def populacao_uf(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=1990, le=2060),
    ano_inicio: Optional[int] = Query(None, ge=1990, le=2060),
    ano_fim: Optional[int] = Query(None, ge=1990, le=2060),
):
    clauses: List[str] = []
    params: List[Any] = []
    if uf:
        clauses.append("uf = ?")
        params.append(uf.upper())
    if ano is not None:
        clauses.append("ano = ?")
        params.append(ano)
    else:
        if ano_inicio is not None:
            clauses.append("ano >= ?")
            params.append(ano_inicio)
        if ano_fim is not None:
            clauses.append("ano <= ?")
            params.append(ano_fim)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    sql = (
        f"SELECT uf, uf_nome, ano, populacao_projetada "
        f"FROM populacao_projecao_uf {where} "
        f"ORDER BY uf, ano"
    )
    rows = query(sql, tuple(params))
    return _envelope(
        "populacao.uf", rows,
        uf=uf.upper() if uf else None,
        ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )
