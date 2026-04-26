from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Path, Query

from ..auth import require_api_key
from ..db import query

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/datasus/sinan",
    tags=["SINAN - Agravos de Notificacao"],
    dependencies=[Depends(require_api_key)],
)

FONTE = "SINAN/DATASUS"
TABELA = "insights_sinan_mensal"


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


_SCHEMA_CACHE: Optional[List[str]] = None


def _columns() -> List[str]:
    """Inspeciona as colunas da tabela em runtime e cacheia."""
    global _SCHEMA_CACHE
    if _SCHEMA_CACHE is not None:
        return _SCHEMA_CACHE
    try:
        rows = query(f"PRAGMA table_info({TABELA})")
        cols = [r["name"] for r in rows] if rows else []
        _SCHEMA_CACHE = cols
        if not cols:
            logger.warning(f"Tabela {TABELA} sem colunas (ou inexistente).")
        return cols
    except Exception as e:
        logger.error(f"Falha ao inspecionar {TABELA}: {e}")
        _SCHEMA_CACHE = []
        return []


def _has(col: str) -> bool:
    return col in _columns()


def _build_filters(
    uf: Optional[str],
    ano: Optional[int],
    ano_inicio: Optional[int],
    ano_fim: Optional[int],
    municipio_ibge: Optional[str],
    agravo: Optional[str] = None,
) -> Tuple[str, List[Any]]:
    clauses: List[str] = []
    params: List[Any] = []
    if uf and _has("uf"):
        clauses.append("uf = ?")
        params.append(uf.upper())
    if ano is not None and _has("ano"):
        clauses.append("ano = ?")
        params.append(ano)
    else:
        if ano_inicio is not None and _has("ano"):
            clauses.append("ano >= ?")
            params.append(ano_inicio)
        if ano_fim is not None and _has("ano"):
            clauses.append("ano <= ?")
            params.append(ano_fim)
    if municipio_ibge and _has("municipio_ibge"):
        clauses.append("municipio_ibge = ?")
        params.append(municipio_ibge)
    if agravo and _has("agravo"):
        clauses.append("agravo = ?")
        params.append(agravo.upper())
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, params


def _envelope(indicador: str, dados: Any, **kwargs) -> Dict[str, Any]:
    out: Dict[str, Any] = {"ok": True, "sistema": "sinan", "indicador": indicador}
    for k, v in kwargs.items():
        if v is not None:
            out[k] = v.upper() if k == "uf" and isinstance(v, str) else v
    if isinstance(dados, list):
        out["total"] = len(dados)
    out["dados"] = dados
    out["metadata"] = {"fonte": FONTE, "atualizado_em": _now_iso()}
    return out


def _casos_col() -> str:
    return "casos" if _has("casos") else ("total_casos" if _has("total_casos") else "casos")


def _obitos_col() -> Optional[str]:
    if _has("obitos"):
        return "obitos"
    if _has("total_obitos"):
        return "total_obitos"
    return None


@router.get("/partitions", summary="Particoes UF x Ano disponiveis")
def partitions():
    cols = _columns()
    if not cols:
        raise HTTPException(500, detail=f"Tabela {TABELA} indisponivel.")
    casos = _casos_col()
    sql = (
        f"SELECT uf, ano, SUM({casos}) AS total_casos, COUNT(*) AS linhas "
        f"FROM {TABELA} GROUP BY uf, ano ORDER BY uf, ano"
    )
    try:
        rows = query(sql)
    except Exception as e:
        raise HTTPException(500, detail=f"Erro consultando partitions: {e}")
    return _envelope("partitions", rows)


@router.get("/agravos", summary="Lista agravos disponiveis com totais")
def agravos(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
):
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim, municipio_ibge)
    casos = _casos_col()
    sql = (
        f"SELECT agravo, SUM({casos}) AS total_casos "
        f"FROM {TABELA} {where} "
        f"GROUP BY agravo ORDER BY total_casos DESC"
    )
    rows = query(sql, tuple(params))
    return _envelope("agravos", rows, uf=uf, municipio_ibge=municipio_ibge, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim)


@router.get("/{agravo}/serie-mensal", summary="Serie mensal por agravo")
def serie_mensal(
    agravo: str = Path(..., description="Codigo do agravo (DENG, TUBE, HANS, etc.)"),
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
):
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim, municipio_ibge, agravo=agravo)
    casos = _casos_col()
    obitos = _obitos_col()
    select_obitos = f", SUM({obitos}) AS total_obitos" if obitos else ""
    sql = (
        f"SELECT ano, mes, SUM({casos}) AS total_casos{select_obitos} "
        f"FROM {TABELA} {where} "
        f"GROUP BY ano, mes ORDER BY ano, mes"
    )
    rows = query(sql, tuple(params))
    return _envelope(f"{agravo.lower()}.serie-mensal", rows, uf=uf, municipio_ibge=municipio_ibge, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim, agravo=agravo.upper())


@router.get("/{agravo}/casos", summary="Resumo casos: total, obitos, letalidade, tendencia")
def casos_resumo(
    agravo: str = Path(...),
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
):
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim, municipio_ibge, agravo=agravo)
    casos = _casos_col()
    obitos = _obitos_col()
    select_obitos = f", SUM({obitos}) AS total_obitos" if obitos else ""
    sql_total = f"SELECT SUM({casos}) AS total_casos{select_obitos} FROM {TABELA} {where}"
    total = query(sql_total, tuple(params))
    total_row = total[0] if total else {}
    total_casos = total_row.get("total_casos") or 0
    total_obitos = total_row.get("total_obitos") if obitos else None
    letalidade = round(100.0 * (total_obitos or 0) / total_casos, 2) if (total_casos and total_obitos is not None) else None

    # Tendencia: serie por ano
    sql_serie_ano = (
        f"SELECT ano, SUM({casos}) AS total_casos "
        f"FROM {TABELA} {where} "
        f"GROUP BY ano ORDER BY ano"
    )
    serie_ano = query(sql_serie_ano, tuple(params))
    tendencia = None
    if len(serie_ano) >= 2:
        primeiro = serie_ano[0]["total_casos"] or 0
        ultimo = serie_ano[-1]["total_casos"] or 0
        if primeiro:
            tendencia = round(100.0 * (ultimo - primeiro) / primeiro, 2)

    dados = {
        "total_casos": total_casos,
        "total_obitos": total_obitos,
        "letalidade_pct": letalidade,
        "tendencia_pct": tendencia,
        "serie_anual": serie_ano,
    }
    return _envelope(f"{agravo.lower()}.casos", dados, uf=uf, municipio_ibge=municipio_ibge, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim, agravo=agravo.upper())


@router.get("/dengue/incidencia", summary="Incidencia de dengue por 100k habitantes")
def dengue_incidencia(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
):
    # TODO: cruzar com tabela populacao_municipio para calcular incidencia/100k.
    # Por ora retorna apenas os casos absolutos.
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim, municipio_ibge, agravo="DENG")
    casos = _casos_col()
    sql = (
        f"SELECT ano, SUM({casos}) AS total_casos "
        f"FROM {TABELA} {where} "
        f"GROUP BY ano ORDER BY ano"
    )
    try:
        rows = query(sql, tuple(params))
    except Exception as e:
        raise HTTPException(500, detail=f"Erro: {e}")
    return _envelope("dengue.incidencia", {"casos_anuais": rows, "incidencia_por_100k": None, "obs": "TODO: cruzar com populacao_municipio"}, uf=uf, municipio_ibge=municipio_ibge, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim)
