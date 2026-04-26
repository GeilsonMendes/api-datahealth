from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query

from ..auth import require_api_key
from ..db import query

router = APIRouter(
    prefix="/datasus/oncologia",
    tags=["Oncologia Feminina - SISCOLO + SISMAMA + SISCAN"],
    dependencies=[Depends(require_api_key)],
)

FONTE = "SISCOLO+SISMAMA+SISCAN/DATASUS"


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _build_filters(
    uf: Optional[str],
    ano: Optional[int],
    ano_inicio: Optional[int],
    ano_fim: Optional[int],
    municipio_ibge: Optional[str],
    extra: Optional[List[Tuple[str, Any]]] = None,
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
    if municipio_ibge:
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
        "sistema": "oncologia",
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


@router.get("/partitions", summary="Particoes UF x Ano (colo + mama)")
def partitions():
    try:
        colo = query(
            "SELECT uf, ano, SUM(total_exames) AS total_exames, COUNT(*) AS linhas "
            "FROM oncologia_colo_mensal GROUP BY uf, ano ORDER BY uf, ano"
        )
        mama = query(
            "SELECT uf, ano, SUM(total_exames) AS total_exames, COUNT(*) AS linhas "
            "FROM oncologia_mama_mensal GROUP BY uf, ano ORDER BY uf, ano"
        )
    except Exception as e:
        raise HTTPException(500, detail=f"Erro consultando partitions: {e}")
    return _envelope("partitions", {"colo": colo, "mama": mama})


def _serie_mensal(tabela: str, **kwargs):
    where, params = _build_filters(
        kwargs.get("uf"), kwargs.get("ano"),
        kwargs.get("ano_inicio"), kwargs.get("ano_fim"),
        kwargs.get("municipio_ibge"),
    )
    sql = (
        f"SELECT ano, mes, SUM(total_exames) AS total_exames "
        f"FROM {tabela} {where} "
        f"GROUP BY ano, mes ORDER BY ano, mes"
    )
    return query(sql, tuple(params))


def _resultados(tabela: str, **kwargs):
    where, params = _build_filters(
        kwargs.get("uf"), kwargs.get("ano"),
        kwargs.get("ano_inicio"), kwargs.get("ano_fim"),
        kwargs.get("municipio_ibge"),
    )
    sql = (
        f"SELECT resultado_grupo, SUM(total_exames) AS total_exames "
        f"FROM {tabela} {where} "
        f"GROUP BY resultado_grupo ORDER BY total_exames DESC"
    )
    rows = query(sql, tuple(params))
    total = sum(r["total_exames"] or 0 for r in rows) or 1
    for r in rows:
        r["pct"] = round(100.0 * (r["total_exames"] or 0) / total, 2)
    return rows


def _faixas(tabela: str, **kwargs):
    where, params = _build_filters(
        kwargs.get("uf"), kwargs.get("ano"),
        kwargs.get("ano_inicio"), kwargs.get("ano_fim"),
        kwargs.get("municipio_ibge"),
    )
    sql = (
        f"SELECT faixa_etaria, SUM(total_exames) AS total_exames "
        f"FROM {tabela} {where} "
        f"GROUP BY faixa_etaria ORDER BY total_exames DESC"
    )
    rows = query(sql, tuple(params))
    total = sum(r["total_exames"] or 0 for r in rows) or 1
    for r in rows:
        r["pct"] = round(100.0 * (r["total_exames"] or 0) / total, 2)
    return rows


# COLO
@router.get("/colo/serie-mensal", summary="Serie mensal exames colo")
def colo_serie(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
):
    rows = _serie_mensal("oncologia_colo_mensal", uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim, municipio_ibge=municipio_ibge)
    return _envelope("colo.serie-mensal", rows, uf=uf, municipio_ibge=municipio_ibge, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim)


@router.get("/colo/resultados", summary="Top resultado_grupo (colo)")
def colo_resultados(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
):
    rows = _resultados("oncologia_colo_mensal", uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim, municipio_ibge=municipio_ibge)
    return _envelope("colo.resultados", rows, uf=uf, municipio_ibge=municipio_ibge, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim)


@router.get("/colo/faixa-etaria", summary="Top faixa etaria (colo)")
def colo_faixa(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
):
    rows = _faixas("oncologia_colo_mensal", uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim, municipio_ibge=municipio_ibge)
    return _envelope("colo.faixa-etaria", rows, uf=uf, municipio_ibge=municipio_ibge, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim)


# MAMA
@router.get("/mama/serie-mensal", summary="Serie mensal exames mama")
def mama_serie(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
):
    rows = _serie_mensal("oncologia_mama_mensal", uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim, municipio_ibge=municipio_ibge)
    return _envelope("mama.serie-mensal", rows, uf=uf, municipio_ibge=municipio_ibge, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim)


@router.get("/mama/birads", summary="Top resultado_grupo BIRADS (mama)")
def mama_birads(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
):
    rows = _resultados("oncologia_mama_mensal", uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim, municipio_ibge=municipio_ibge)
    return _envelope("mama.birads", rows, uf=uf, municipio_ibge=municipio_ibge, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim)


@router.get("/resumo", summary="Resumo combinado colo + mama")
def resumo(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
):
    colo_serie_d = _serie_mensal("oncologia_colo_mensal", uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim, municipio_ibge=municipio_ibge)
    colo_res = _resultados("oncologia_colo_mensal", uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim, municipio_ibge=municipio_ibge)
    mama_serie_d = _serie_mensal("oncologia_mama_mensal", uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim, municipio_ibge=municipio_ibge)
    mama_res = _resultados("oncologia_mama_mensal", uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim, municipio_ibge=municipio_ibge)

    # pct_alterados colo: atipias + LSIL + HSIL + cancer
    grupos_alterados_colo = {"atipias", "lsil", "hsil", "cancer"}
    total_colo = sum(r["total_exames"] or 0 for r in colo_res) or 1
    alt_colo = sum(
        (r["total_exames"] or 0) for r in colo_res
        if (r.get("resultado_grupo") or "").lower() in grupos_alterados_colo
    )
    pct_alterados_colo = round(100.0 * alt_colo / total_colo, 2)

    # pct_alterados mama: birads_4 + birads_5 + birads_6
    total_mama = sum(r["total_exames"] or 0 for r in mama_res) or 1
    alt_mama = sum(
        (r["total_exames"] or 0) for r in mama_res
        if (r.get("resultado_grupo") or "").lower() in {"birads_4", "birads_5", "birads_6"}
    )
    pct_alterados_mama = round(100.0 * alt_mama / total_mama, 2)

    return {
        "ok": True,
        "sistema": "oncologia",
        "indicador": "resumo",
        "uf": uf.upper() if uf else None,
        "municipio_ibge": municipio_ibge,
        "ano": ano,
        "ano_inicio": ano_inicio,
        "ano_fim": ano_fim,
        "dados": {
            "colo": {
                "serie_mensal": colo_serie_d,
                "resultados": colo_res,
                "pct_alterados": pct_alterados_colo,
            },
            "mama": {
                "serie_mensal": mama_serie_d,
                "birads": mama_res,
                "pct_alterados": pct_alterados_mama,
            },
        },
        "metadata": {"fonte": FONTE, "atualizado_em": _now_iso()},
    }
