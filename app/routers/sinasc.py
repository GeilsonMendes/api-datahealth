from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query

from ..auth import require_api_key
from ..db import query

router = APIRouter(
    prefix="/datasus/sinasc",
    tags=["SINASC - Nascidos Vivos"],
    dependencies=[Depends(require_api_key)],
)

FONTE = "SINASC/DATASUS"


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
    dados: List[Dict[str, Any]],
    *,
    uf: Optional[str] = None,
    municipio_ibge: Optional[str] = None,
    ano: Optional[int] = None,
    ano_inicio: Optional[int] = None,
    ano_fim: Optional[int] = None,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "ok": True,
        "sistema": "sinasc",
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
    out["total"] = len(dados)
    out["dados"] = dados
    out["metadata"] = {"fonte": FONTE, "atualizado_em": _now_iso()}
    return out


# ---------------------------------------------------------------------------
# /partitions
# ---------------------------------------------------------------------------

@router.get("/partitions", summary="Listar particoes UF x Ano disponiveis")
def partitions():
    sql = (
        "SELECT uf, ano, SUM(nascimentos) AS total_nascimentos, COUNT(*) AS linhas "
        "FROM insights_sinasc_resumo_mensal "
        "GROUP BY uf, ano ORDER BY uf, ano"
    )
    try:
        rows = query(sql)
    except Exception as e:
        raise HTTPException(500, detail=f"Erro consultando partitions: {e}")
    return _envelope("partitions", rows)


# ---------------------------------------------------------------------------
# /nascimentos/serie-mensal
# ---------------------------------------------------------------------------

@router.get("/nascimentos/serie-mensal", summary="Serie mensal de nascimentos")
def serie_mensal(
    uf: Optional[str] = Query(None, min_length=2, max_length=2, description="UF (ex: CE)"),
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
        f"SUM(vaginais) AS vaginais, "
        f"SUM(baixo_peso) AS baixo_peso, "
        f"SUM(prematuros) AS prematuros, "
        f"SUM(mae_adolescente) AS mae_adolescente, "
        f"SUM(prenatal_adequado) AS prenatal_adequado, "
        f"AVG(peso_medio) AS peso_medio, "
        f"AVG(idade_mae_media) AS idade_mae_media "
        f"FROM insights_sinasc_resumo_mensal {where} "
        f"GROUP BY ano, mes ORDER BY ano, mes"
    )
    rows = query(sql, tuple(params))
    return _envelope(
        "nascimentos.serie-mensal", rows,
        uf=uf, municipio_ibge=municipio_ibge,
        ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )


# ---------------------------------------------------------------------------
# /parto
# ---------------------------------------------------------------------------

@router.get("/parto", summary="Nascimentos por tipo de parto")
def parto(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
):
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim, municipio_ibge)
    sql = (
        f"SELECT tipo_parto, SUM(nascimentos) AS nascimentos "
        f"FROM insights_sinasc_parto_mensal {where} "
        f"GROUP BY tipo_parto ORDER BY nascimentos DESC"
    )
    rows = query(sql, tuple(params))
    total_geral = sum(r["nascimentos"] or 0 for r in rows) or 1
    for r in rows:
        r["pct"] = round(100.0 * (r["nascimentos"] or 0) / total_geral, 2)
    return _envelope(
        "parto", rows,
        uf=uf, municipio_ibge=municipio_ibge,
        ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )


# ---------------------------------------------------------------------------
# /baixo-peso
# ---------------------------------------------------------------------------

@router.get("/baixo-peso", summary="Nascimentos por faixa de peso")
def baixo_peso(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
):
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim, municipio_ibge)
    sql = (
        f"SELECT faixa_peso, SUM(nascimentos) AS nascimentos "
        f"FROM insights_sinasc_peso_mensal {where} "
        f"GROUP BY faixa_peso ORDER BY nascimentos DESC"
    )
    rows = query(sql, tuple(params))
    total_geral = sum(r["nascimentos"] or 0 for r in rows) or 1
    for r in rows:
        r["pct"] = round(100.0 * (r["nascimentos"] or 0) / total_geral, 2)
    return _envelope(
        "baixo-peso", rows,
        uf=uf, municipio_ibge=municipio_ibge,
        ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )


# ---------------------------------------------------------------------------
# /mae-adolescente
# ---------------------------------------------------------------------------

@router.get("/mae-adolescente", summary="Taxa de nascimentos de maes adolescentes")
def mae_adolescente(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
):
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim, municipio_ibge)
    sql = (
        f"SELECT ano, mes, "
        f"SUM(mae_adolescente) AS mae_adolescente, "
        f"SUM(nascimentos) AS nascimentos, "
        f"CASE WHEN SUM(nascimentos) > 0 "
        f"THEN ROUND(100.0 * SUM(mae_adolescente) / SUM(nascimentos), 2) "
        f"ELSE 0 END AS pct_mae_adolescente "
        f"FROM insights_sinasc_resumo_mensal {where} "
        f"GROUP BY ano, mes ORDER BY ano, mes"
    )
    rows = query(sql, tuple(params))
    return _envelope(
        "mae-adolescente", rows,
        uf=uf, municipio_ibge=municipio_ibge,
        ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )


# ---------------------------------------------------------------------------
# /prenatal
# ---------------------------------------------------------------------------

@router.get("/prenatal", summary="Nascimentos por consultas de pre-natal")
def prenatal(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
):
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim, municipio_ibge)
    sql = (
        f"SELECT consultas_prenatal, SUM(nascimentos) AS nascimentos "
        f"FROM insights_sinasc_prenatal_mensal {where} "
        f"GROUP BY consultas_prenatal ORDER BY nascimentos DESC"
    )
    rows = query(sql, tuple(params))
    total_geral = sum(r["nascimentos"] or 0 for r in rows) or 1
    for r in rows:
        r["pct"] = round(100.0 * (r["nascimentos"] or 0) / total_geral, 2)
    return _envelope(
        "prenatal", rows,
        uf=uf, municipio_ibge=municipio_ibge,
        ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )


# ---------------------------------------------------------------------------
# /idade-mae
# ---------------------------------------------------------------------------

@router.get("/idade-mae", summary="Nascimentos por faixa de idade da mae")
def idade_mae(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
):
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim, municipio_ibge)
    sql = (
        f"SELECT faixa_idade_mae, SUM(nascimentos) AS nascimentos "
        f"FROM insights_sinasc_mae_mensal {where} "
        f"GROUP BY faixa_idade_mae ORDER BY nascimentos DESC"
    )
    rows = query(sql, tuple(params))
    total_geral = sum(r["nascimentos"] or 0 for r in rows) or 1
    for r in rows:
        r["pct"] = round(100.0 * (r["nascimentos"] or 0) / total_geral, 2)
    return _envelope(
        "idade-mae", rows,
        uf=uf, municipio_ibge=municipio_ibge,
        ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )


# ---------------------------------------------------------------------------
# /cesarea (RIPSA F.8 - meta <=30%)
# ---------------------------------------------------------------------------

@router.get("/cesarea", summary="Taxa de cesarea (RIPSA F.8 - meta <=30%)")
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
        f"SUM(cesareos) AS cesareos, "
        f"SUM(nascimentos) AS nascimentos, "
        f"CASE WHEN SUM(nascimentos) > 0 "
        f"THEN ROUND(100.0 * SUM(cesareos) / SUM(nascimentos), 2) "
        f"ELSE 0 END AS pct_cesarea "
        f"FROM insights_sinasc_resumo_mensal {where} "
        f"GROUP BY ano, mes ORDER BY ano, mes"
    )
    rows = query(sql, tuple(params))
    total_cesareos = sum(r["cesareos"] or 0 for r in rows)
    total_nasc = sum(r["nascimentos"] or 0 for r in rows)
    pct_geral = round(100.0 * total_cesareos / total_nasc, 2) if total_nasc > 0 else 0
    out = _envelope(
        "cesarea", rows,
        uf=uf, municipio_ibge=municipio_ibge,
        ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )
    out["total_cesareos"] = total_cesareos
    out["total_nascimentos"] = total_nasc
    out["pct_cesarea_geral"] = pct_geral
    out["meta_ripsa_f8"] = 30.0
    return out


# ---------------------------------------------------------------------------
# /resumo (combina todos)
# ---------------------------------------------------------------------------

@router.get("/resumo", summary="Resumo agregado: serie + parto + peso + prenatal + idade-mae + cesarea")
def resumo(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
):
    serie = serie_mensal(uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim, municipio_ibge=municipio_ibge)
    prt = parto(uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim, municipio_ibge=municipio_ibge)
    peso = baixo_peso(uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim, municipio_ibge=municipio_ibge)
    pre = prenatal(uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim, municipio_ibge=municipio_ibge)
    idade = idade_mae(uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim, municipio_ibge=municipio_ibge)
    ces = cesarea(uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim, municipio_ibge=municipio_ibge)
    adoles = mae_adolescente(uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim, municipio_ibge=municipio_ibge)
    total_nasc = sum(r["nascimentos"] or 0 for r in serie["dados"])
    return {
        "ok": True,
        "sistema": "sinasc",
        "indicador": "resumo",
        "uf": uf.upper() if uf else None,
        "municipio_ibge": municipio_ibge,
        "ano": ano,
        "ano_inicio": ano_inicio,
        "ano_fim": ano_fim,
        "total_nascimentos": total_nasc,
        "pct_cesarea_geral": ces.get("pct_cesarea_geral"),
        "meta_ripsa_f8": 30.0,
        "dados": {
            "serie_mensal": serie["dados"],
            "parto": prt["dados"],
            "baixo_peso": peso["dados"],
            "prenatal": pre["dados"],
            "idade_mae": idade["dados"],
            "cesarea": ces["dados"],
            "mae_adolescente": adoles["dados"],
        },
        "metadata": {"fonte": FONTE, "atualizado_em": _now_iso()},
    }
