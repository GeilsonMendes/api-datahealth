from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query

from ..auth import require_api_key
from ..db import query

router = APIRouter(
    prefix="/datasus/pni",
    tags=["PNI - Programa Nacional de Imunizacoes"],
    dependencies=[Depends(require_api_key)],
)

FONTE = "PNI/DATASUS"


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _build_filters(
    uf: Optional[str],
    ano: Optional[int],
    ano_inicio: Optional[int],
    ano_fim: Optional[int],
    municipio_ibge: Optional[str],
    extra: Optional[List[Tuple[str, Any]]] = None,
    has_municipio: bool = True,
) -> Tuple[str, List[Any]]:
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
    if municipio_ibge and has_municipio:
        clauses.append("municipio_ibge = ?")
        params.append(municipio_ibge)
    if extra:
        for col, val in extra:
            if val is not None and val != "":
                clauses.append(f"{col} = ?")
                params.append(val)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, params


def _envelope(
    indicador: str,
    dados: Any,
    *,
    uf: Optional[str] = None,
    municipio_ibge: Optional[str] = None,
    ano: Optional[int] = None,
    ano_inicio: Optional[int] = None,
    ano_fim: Optional[int] = None,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "ok": True,
        "sistema": "pni",
        "indicador": indicador,
    }
    if uf:
        out["uf"] = uf.upper()
    if municipio_ibge:
        out["municipio_ibge"] = municipio_ibge
    if ano is not None:
        out["ano"] = ano
    if ano_inicio is not None:
        out["ano_inicio"] = ano_inicio
    if ano_fim is not None:
        out["ano_fim"] = ano_fim
    if isinstance(dados, list):
        out["total"] = len(dados)
    out["dados"] = dados
    out["metadata"] = {"fonte": FONTE, "atualizado_em": _now_iso()}
    return out


@router.get("/partitions", summary="Listar particoes UF x Ano disponiveis")
def partitions():
    sql = (
        "SELECT uf, ano, SUM(total_doses) AS total_doses, COUNT(*) AS linhas "
        "FROM pni_doses_mensal "
        "GROUP BY uf, ano ORDER BY uf, ano"
    )
    try:
        rows = query(sql)
    except Exception as e:
        raise HTTPException(500, detail=f"Erro consultando partitions: {e}")
    return _envelope("partitions", rows)


@router.get("/cobertura", summary="Cobertura vacinal por imunobiologico")
def cobertura(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
    imuno: Optional[str] = Query(None, description="Codigo do imunobiologico"),
):
    extra = [("imuno_codigo", imuno)] if imuno else None
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim, municipio_ibge, extra=extra)
    sql = (
        f"SELECT ano, imuno_codigo, imuno_nome, "
        f"AVG(cobertura_pct) AS cobertura_pct_media "
        f"FROM pni_cobertura_anual {where} "
        f"GROUP BY ano, imuno_codigo, imuno_nome "
        f"ORDER BY ano, imuno_codigo"
    )
    rows = query(sql, tuple(params))
    return _envelope(
        "cobertura", rows,
        uf=uf, municipio_ibge=municipio_ibge,
        ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )


@router.get("/doses", summary="Doses aplicadas (serie + top imunos + top faixa etaria)")
def doses(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
    imuno: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=200),
):
    extra = [("imuno_codigo", imuno)] if imuno else None
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim, municipio_ibge, extra=extra)

    sql_serie = (
        f"SELECT ano, mes, SUM(total_doses) AS total_doses "
        f"FROM pni_doses_mensal {where} "
        f"GROUP BY ano, mes ORDER BY ano, mes"
    )
    sql_imunos = (
        f"SELECT imuno_codigo, imuno_nome, SUM(total_doses) AS total_doses "
        f"FROM pni_doses_mensal {where} "
        f"GROUP BY imuno_codigo, imuno_nome ORDER BY total_doses DESC LIMIT ?"
    )
    sql_faixa = (
        f"SELECT faixa_etaria, SUM(total_doses) AS total_doses "
        f"FROM pni_doses_mensal {where} "
        f"GROUP BY faixa_etaria ORDER BY total_doses DESC LIMIT ?"
    )
    serie = query(sql_serie, tuple(params))
    top_imunos = query(sql_imunos, tuple(params) + (limit,))
    top_faixas = query(sql_faixa, tuple(params) + (limit,))
    return _envelope(
        "doses",
        {"serie_mensal": serie, "top_imunos": top_imunos, "top_faixa_etaria": top_faixas},
        uf=uf, municipio_ibge=municipio_ibge,
        ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )


@router.get("/imunos", summary="Lista imunobiologicos disponiveis")
def imunos():
    sql = (
        "SELECT DISTINCT imuno_codigo, imuno_nome "
        "FROM pni_doses_mensal "
        "WHERE imuno_codigo IS NOT NULL "
        "ORDER BY imuno_codigo"
    )
    rows = query(sql)
    return _envelope("imunos", rows)


@router.get("/resumo", summary="Resumo agregado: cobertura + doses")
def resumo(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
    imuno: Optional[str] = Query(None),
):
    cob = cobertura(uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim, municipio_ibge=municipio_ibge, imuno=imuno)
    dos = doses(uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim, municipio_ibge=municipio_ibge, imuno=imuno, limit=20)
    return {
        "ok": True,
        "sistema": "pni",
        "indicador": "resumo",
        "uf": uf.upper() if uf else None,
        "municipio_ibge": municipio_ibge,
        "ano": ano,
        "ano_inicio": ano_inicio,
        "ano_fim": ano_fim,
        "dados": {
            "cobertura": cob["dados"],
            "doses": dos["dados"],
        },
        "metadata": {"fonte": FONTE, "atualizado_em": _now_iso()},
    }
