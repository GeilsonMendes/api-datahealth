from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query

from ..auth import require_api_key
from ..db import query

router = APIRouter(
    prefix="/indicadores",
    tags=["Indicadores Cruzados - RIPSA"],
    dependencies=[Depends(require_api_key)],
)

FONTE = "RIPSA/DATASUS (cruzamento SIM+SINASC+SIH)"


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _build_filters(
    uf: Optional[str],
    ano: Optional[int],
    ano_inicio: Optional[int],
    ano_fim: Optional[int],
    municipio_ibge: Optional[str],
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
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, params


def _envelope(indicador: str, dados: Any, **kwargs) -> Dict[str, Any]:
    out: Dict[str, Any] = {"ok": True, "sistema": "indicadores", "indicador": indicador}
    for k, v in kwargs.items():
        if v is not None:
            out[k] = v.upper() if k == "uf" and isinstance(v, str) else v
    if isinstance(dados, list):
        out["total"] = len(dados)
    out["dados"] = dados
    out["metadata"] = {"fonte": FONTE, "atualizado_em": _now_iso()}
    return out


@router.get("/partitions", summary="Particoes disponiveis (TMI + RMM)")
def partitions():
    try:
        tmi = query("SELECT uf, ano, COUNT(*) AS linhas FROM indicador_tmi_anual GROUP BY uf, ano ORDER BY uf, ano")
        rmm = query("SELECT uf, ano, COUNT(*) AS linhas FROM indicador_rmm_anual GROUP BY uf, ano ORDER BY uf, ano")
    except Exception as e:
        raise HTTPException(500, detail=f"Erro: {e}")
    return _envelope("partitions", {"tmi": tmi, "rmm": rmm})


@router.get("/tmi", summary="Taxa de Mortalidade Infantil (RIPSA C.1)")
def tmi(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
    granularidade: str = Query("anual", pattern="^(anual|mensal)$"),
):
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim, municipio_ibge)
    if granularidade == "mensal":
        sql = (
            f"SELECT ano, mes, "
            f"SUM(obitos_infantis) AS obitos_infantis, "
            f"SUM(nascidos_vivos) AS nascidos_vivos, "
            f"AVG(tmi_por_mil) AS tmi_por_mil "
            f"FROM indicador_tmi_mensal {where} "
            f"GROUP BY ano, mes ORDER BY ano, mes"
        )
    else:
        sql = (
            f"SELECT ano, "
            f"SUM(obitos_infantis) AS obitos_infantis, "
            f"SUM(obitos_neonatal_precoce) AS obitos_neonatal_precoce, "
            f"SUM(obitos_neonatal_tardio) AS obitos_neonatal_tardio, "
            f"SUM(obitos_posneonatal) AS obitos_posneonatal, "
            f"SUM(nascidos_vivos) AS nascidos_vivos, "
            f"AVG(tmi_por_mil) AS tmi_por_mil, "
            f"AVG(tmi_neonatal_precoce) AS tmi_neonatal_precoce, "
            f"AVG(tmi_neonatal_tardio) AS tmi_neonatal_tardio, "
            f"AVG(tmi_posneonatal) AS tmi_posneonatal "
            f"FROM indicador_tmi_anual {where} "
            f"GROUP BY ano ORDER BY ano"
        )
    rows = query(sql, tuple(params))
    return _envelope("tmi", rows, uf=uf, municipio_ibge=municipio_ibge, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim, granularidade=granularidade)


@router.get("/rmm", summary="Razao de Mortalidade Materna (RIPSA C.3)")
def rmm(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
    granularidade: str = Query("anual", pattern="^(anual|mensal)$"),
):
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim, municipio_ibge)
    tabela = "indicador_rmm_mensal" if granularidade == "mensal" else "indicador_rmm_anual"
    group_extra = ", mes" if granularidade == "mensal" else ""
    sql = (
        f"SELECT ano{group_extra}, "
        f"SUM(obitos_maternos) AS obitos_maternos, "
        f"SUM(nascidos_vivos) AS nascidos_vivos, "
        f"AVG(rmm_por_100k) AS rmm_por_100k "
        f"FROM {tabela} {where} "
        f"GROUP BY ano{group_extra} ORDER BY ano{group_extra}"
    )
    rows = query(sql, tuple(params))
    return _envelope("rmm", rows, uf=uf, municipio_ibge=municipio_ibge, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim, granularidade=granularidade)


@router.get("/cesarea", summary="% partos cesareos (RIPSA F.8)")
def cesarea(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
):
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim, municipio_ibge)
    sql = (
        f"SELECT ano, mes, "
        f"SUM(nascimentos) AS nascimentos, "
        f"SUM(cesareos) AS cesareos, "
        f"CASE WHEN SUM(nascimentos) > 0 "
        f"THEN ROUND(100.0 * SUM(cesareos) / SUM(nascimentos), 2) ELSE NULL END AS pct_cesarea "
        f"FROM insights_sinasc_resumo_mensal {where} "
        f"GROUP BY ano, mes ORDER BY ano, mes"
    )
    try:
        rows = query(sql, tuple(params))
    except Exception as e:
        raise HTTPException(500, detail=f"Erro: {e}")
    return _envelope("cesarea", rows, uf=uf, municipio_ibge=municipio_ibge, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim)


@router.get("/prenatal-adequado", summary="% pre-natal adequado (>=7 consultas)")
def prenatal_adequado(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
):
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim, municipio_ibge)
    sql = (
        f"SELECT ano, mes, "
        f"SUM(nascimentos) AS nascimentos, "
        f"SUM(prenatal_adequado) AS prenatal_adequado, "
        f"CASE WHEN SUM(nascimentos) > 0 "
        f"THEN ROUND(100.0 * SUM(prenatal_adequado) / SUM(nascimentos), 2) ELSE NULL END AS pct_prenatal_adequado "
        f"FROM insights_sinasc_resumo_mensal {where} "
        f"GROUP BY ano, mes ORDER BY ano, mes"
    )
    try:
        rows = query(sql, tuple(params))
    except Exception as e:
        raise HTTPException(500, detail=f"Erro: {e}")
    return _envelope("prenatal-adequado", rows, uf=uf, municipio_ibge=municipio_ibge, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim)


@router.get("/baixo-peso", summary="% nascidos vivos com baixo peso (<2500g)")
def baixo_peso(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
):
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim, municipio_ibge)
    sql = (
        f"SELECT ano, mes, "
        f"SUM(nascimentos) AS nascimentos, "
        f"SUM(baixo_peso) AS baixo_peso, "
        f"CASE WHEN SUM(nascimentos) > 0 "
        f"THEN ROUND(100.0 * SUM(baixo_peso) / SUM(nascimentos), 2) ELSE NULL END AS pct_baixo_peso "
        f"FROM insights_sinasc_resumo_mensal {where} "
        f"GROUP BY ano, mes ORDER BY ano, mes"
    )
    try:
        rows = query(sql, tuple(params))
    except Exception as e:
        raise HTTPException(500, detail=f"Erro: {e}")
    return _envelope("baixo-peso", rows, uf=uf, municipio_ibge=municipio_ibge, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim)


@router.get("/letalidade-hospitalar", summary="% letalidade hospitalar (RIPSA D.13)")
def letalidade_hospitalar(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
):
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim, municipio_ibge)
    sql = (
        f"SELECT ano, mes, cid_capitulo, "
        f"SUM(aihs) AS aihs, "
        f"SUM(obitos) AS obitos, "
        f"CASE WHEN SUM(aihs) > 0 "
        f"THEN ROUND(100.0 * SUM(obitos) / SUM(aihs), 2) ELSE NULL END AS pct_letalidade "
        f"FROM insights_sih_capitulo_mensal {where} "
        f"GROUP BY ano, mes, cid_capitulo ORDER BY ano, mes, cid_capitulo"
    )
    try:
        rows = query(sql, tuple(params))
    except Exception as e:
        raise HTTPException(500, detail=f"Erro: {e}")
    return _envelope("letalidade-hospitalar", rows, uf=uf, municipio_ibge=municipio_ibge, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim)


@router.get("/catalogo", summary="Catalogo de indicadores RIPSA disponiveis")
def catalogo():
    indicadores = [
        {
            "codigo": "C.1",
            "nome": "Taxa de Mortalidade Infantil (TMI)",
            "formula": "(obitos < 1 ano / nascidos vivos) * 1.000",
            "fonte": "SIM + SINASC",
            "meta": "OMS: < 10 por mil",
            "endpoint": "/indicadores/tmi",
        },
        {
            "codigo": "C.1.1",
            "nome": "TMI Neonatal Precoce (0-6 dias)",
            "formula": "(obitos 0-6d / nascidos vivos) * 1.000",
            "fonte": "SIM + SINASC",
            "meta": None,
            "endpoint": "/indicadores/tmi?granularidade=anual",
        },
        {
            "codigo": "C.1.2",
            "nome": "TMI Neonatal Tardia (7-27 dias)",
            "formula": "(obitos 7-27d / nascidos vivos) * 1.000",
            "fonte": "SIM + SINASC",
            "meta": None,
            "endpoint": "/indicadores/tmi?granularidade=anual",
        },
        {
            "codigo": "C.1.3",
            "nome": "TMI Pos-Neonatal (28d-1 ano)",
            "formula": "(obitos 28d-1a / nascidos vivos) * 1.000",
            "fonte": "SIM + SINASC",
            "meta": None,
            "endpoint": "/indicadores/tmi?granularidade=anual",
        },
        {
            "codigo": "C.3",
            "nome": "Razao de Mortalidade Materna (RMM)",
            "formula": "(obitos maternos / nascidos vivos) * 100.000 (correcao 1,42)",
            "fonte": "SIM + SINASC",
            "meta": "ODS: < 30 por 100k em 2030",
            "endpoint": "/indicadores/rmm",
        },
        {
            "codigo": "F.8",
            "nome": "Proporcao de Partos Cesareos",
            "formula": "(cesareos / nascimentos) * 100",
            "fonte": "SINASC",
            "meta": "OMS: ate 15%",
            "endpoint": "/indicadores/cesarea",
        },
        {
            "codigo": "F.6",
            "nome": "% Pre-natal Adequado (>=7 consultas)",
            "formula": "(prenatal_adequado / nascimentos) * 100",
            "fonte": "SINASC",
            "meta": "Previne Brasil: >= 60%",
            "endpoint": "/indicadores/prenatal-adequado",
        },
        {
            "codigo": "F.10",
            "nome": "% Nascidos Vivos com Baixo Peso (<2500g)",
            "formula": "(baixo_peso / nascimentos) * 100",
            "fonte": "SINASC",
            "meta": "OMS: < 10%",
            "endpoint": "/indicadores/baixo-peso",
        },
        {
            "codigo": "D.13",
            "nome": "Taxa de Letalidade Hospitalar",
            "formula": "(obitos / aihs) * 100",
            "fonte": "SIH",
            "meta": None,
            "endpoint": "/indicadores/letalidade-hospitalar",
        },
    ]
    return _envelope("catalogo", indicadores)
