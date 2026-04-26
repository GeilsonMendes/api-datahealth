from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query

from ..auth import require_api_key
from ..db import query

router = APIRouter(
    prefix="/datasus/sih",
    tags=["SIH - Internacoes Hospitalares SUS"],
    dependencies=[Depends(require_api_key)],
)

FONTE = "SIH/SUS"


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
        "sistema": "sih",
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


CAP_MAP = {
    "I": ["A", "B"], "II": ["C", "D"], "III": ["D"], "IV": ["E"],
    "V": ["F"], "VI": ["G"], "VII": ["H"], "VIII": ["H"],
    "IX": ["I"], "X": ["J"], "XI": ["K"], "XII": ["L"],
    "XIII": ["M"], "XIV": ["N"], "XV": ["O"], "XVI": ["P"],
    "XVII": ["Q"], "XVIII": ["R"], "XIX": ["S", "T"],
    "XX": ["V", "W", "X", "Y"], "XXI": ["Z"], "XXII": ["U"],
}


# ---------------------------------------------------------------------------
# /partitions
# ---------------------------------------------------------------------------

@router.get("/partitions", summary="Listar particoes UF x Ano disponiveis")
def partitions():
    sql = (
        "SELECT uf, ano, SUM(aihs) AS total_aihs, COUNT(*) AS linhas "
        "FROM insights_sih_capitulo_mensal "
        "GROUP BY uf, ano ORDER BY uf, ano"
    )
    try:
        rows = query(sql)
    except Exception as e:
        raise HTTPException(500, detail=f"Erro consultando partitions: {e}")
    return _envelope("partitions", rows)


# ---------------------------------------------------------------------------
# /internacoes/serie-mensal
# ---------------------------------------------------------------------------

@router.get("/internacoes/serie-mensal", summary="Serie mensal de internacoes")
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
        f"SUM(aihs) AS total_aihs, "
        f"SUM(obitos) AS obitos, "
        f"SUM(valor_total) AS valor_total "
        f"FROM insights_sih_capitulo_mensal {where} "
        f"GROUP BY ano, mes ORDER BY ano, mes"
    )
    rows = query(sql, tuple(params))
    return _envelope(
        "internacoes.serie-mensal", rows,
        uf=uf, municipio_ibge=municipio_ibge,
        ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )


# ---------------------------------------------------------------------------
# /internacoes/capitulos
# ---------------------------------------------------------------------------

@router.get("/internacoes/capitulos", summary="Internacoes por capitulo CID-10")
def capitulos(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
    agrupar_por_ano: bool = Query(False),
):
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim, municipio_ibge)
    group_year = ", ano" if agrupar_por_ano else ""
    select_year = ", ano" if agrupar_por_ano else ""
    sql = (
        f"SELECT cid_capitulo{select_year}, "
        f"SUM(aihs) AS total_aihs, "
        f"SUM(obitos) AS total_obitos, "
        f"SUM(valor_total) AS valor_total "
        f"FROM insights_sih_capitulo_mensal {where} "
        f"GROUP BY cid_capitulo{group_year} "
        f"ORDER BY total_aihs DESC"
    )
    rows = query(sql, tuple(params))
    total_geral = sum(r["total_aihs"] or 0 for r in rows) or 1
    for r in rows:
        r["pct"] = round(100.0 * (r["total_aihs"] or 0) / total_geral, 2)
    return _envelope(
        "internacoes.capitulos", rows,
        uf=uf, municipio_ibge=municipio_ibge,
        ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )


# ---------------------------------------------------------------------------
# /internacoes/causas
# ---------------------------------------------------------------------------

@router.get("/internacoes/causas", summary="Causas de internacao (CID-10 3 caracteres)")
def causas(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
    cid10_capitulo: Optional[str] = Query(None, description="Capitulo CID-10 (ex: IX)"),
    limit: int = Query(50, ge=1, le=10000),
    agrupar_por_ano: bool = Query(False),
):
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim, municipio_ibge)

    if cid10_capitulo:
        prefixos = CAP_MAP.get(cid10_capitulo.upper(), [])
        if prefixos:
            ph = ",".join("?" for _ in prefixos)
            cap_clause = f" SUBSTR(cid_3car, 1, 1) IN ({ph})"
            if where:
                where = where + " AND" + cap_clause
            else:
                where = "WHERE" + cap_clause
            params = params + prefixos

    group_year = ", ano" if agrupar_por_ano else ""
    select_year = ", ano" if agrupar_por_ano else ""
    sql = (
        f"SELECT cid_3car{select_year}, "
        f"SUM(aihs) AS total_aihs, "
        f"SUM(obitos) AS total_obitos, "
        f"SUM(valor_total) AS valor_total "
        f"FROM insights_sih_cid_3car_mensal {where} "
        f"GROUP BY cid_3car{group_year} "
        f"ORDER BY total_aihs DESC LIMIT ?"
    )
    rows = query(sql, tuple(params) + (limit,))
    return _envelope(
        "internacoes.causas", rows,
        uf=uf, municipio_ibge=municipio_ibge,
        ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )


# ---------------------------------------------------------------------------
# /uti/serie-mensal
# ---------------------------------------------------------------------------

@router.get("/uti/serie-mensal", summary="Serie mensal de UTI")
def uti_serie_mensal(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
):
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim, municipio_ibge)
    sql = (
        f"SELECT ano, mes, "
        f"SUM(aihs) AS total_aihs, "
        f"SUM(aihs_uti) AS aihs_uti, "
        f"SUM(obitos_uti) AS obitos_uti, "
        f"SUM(dias_uti_total) AS dias_uti, "
        f"SUM(valor_uti) AS valor_uti "
        f"FROM insights_sih_uti_mensal {where} "
        f"GROUP BY ano, mes ORDER BY ano, mes"
    )
    rows = query(sql, tuple(params))
    for r in rows:
        aihs = r["total_aihs"] or 0
        aihs_uti = r["aihs_uti"] or 0
        obitos_uti = r["obitos_uti"] or 0
        r["taxa_uti"] = round(100.0 * aihs_uti / aihs, 2) if aihs else 0.0
        r["letalidade_uti"] = round(100.0 * obitos_uti / aihs_uti, 2) if aihs_uti else 0.0
    return _envelope(
        "uti.serie-mensal", rows,
        uf=uf, municipio_ibge=municipio_ibge,
        ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )


# ---------------------------------------------------------------------------
# /letalidade
# ---------------------------------------------------------------------------

@router.get("/letalidade", summary="Letalidade hospitalar geral e por capitulo")
def letalidade(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
):
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim, municipio_ibge)

    sql_geral = (
        f"SELECT SUM(aihs) AS total_aihs, SUM(obitos) AS total_obitos "
        f"FROM insights_sih_capitulo_mensal {where}"
    )
    geral = query(sql_geral, tuple(params))
    g = geral[0] if geral else {"total_aihs": 0, "total_obitos": 0}
    aihs = g["total_aihs"] or 0
    obitos = g["total_obitos"] or 0
    let_geral = round(100.0 * obitos / aihs, 2) if aihs else 0.0

    sql_cap = (
        f"SELECT cid_capitulo, SUM(aihs) AS total_aihs, SUM(obitos) AS total_obitos "
        f"FROM insights_sih_capitulo_mensal {where} "
        f"GROUP BY cid_capitulo ORDER BY total_aihs DESC LIMIT 10"
    )
    rows = query(sql_cap, tuple(params))
    for r in rows:
        a = r["total_aihs"] or 0
        o = r["total_obitos"] or 0
        r["letalidade"] = round(100.0 * o / a, 2) if a else 0.0

    out = _envelope(
        "letalidade", rows,
        uf=uf, municipio_ibge=municipio_ibge,
        ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )
    out["letalidade_geral"] = let_geral
    out["total_aihs"] = aihs
    out["total_obitos"] = obitos
    return out


# ---------------------------------------------------------------------------
# /perfil
# ---------------------------------------------------------------------------

@router.get("/perfil", summary="Perfil demografico (sexo, faixa etaria, raca)")
def perfil(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
    sexo: Optional[str] = Query(None),
    agrupar_por_ano: bool = Query(False),
):
    extra = [("sexo", sexo)] if sexo else None
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim, municipio_ibge, extra=extra)
    group_year = ", ano" if agrupar_por_ano else ""
    select_year = ", ano" if agrupar_por_ano else ""
    sql = (
        f"SELECT sexo, faixa_etaria, raca{select_year}, "
        f"SUM(aihs) AS total_aihs, "
        f"SUM(obitos) AS total_obitos "
        f"FROM insights_sih_demografico_mensal {where} "
        f"GROUP BY sexo, faixa_etaria, raca{group_year} "
        f"ORDER BY total_aihs DESC"
    )
    rows = query(sql, tuple(params))
    return _envelope(
        "perfil", rows,
        uf=uf, municipio_ibge=municipio_ibge,
        ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )


# ---------------------------------------------------------------------------
# /hospitais
# ---------------------------------------------------------------------------

@router.get("/hospitais", summary="Top hospitais por volume de internacoes")
def hospitais(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
    cnes: Optional[str] = Query(None, description="Filtrar CNES especifico"),
    limit: int = Query(100, ge=1, le=5000),
):
    extra = [("cnes", cnes)] if cnes else None
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim, municipio_ibge, extra=extra)
    sql = (
        f"SELECT cnes, "
        f"SUM(aihs) AS total_aihs, "
        f"SUM(obitos) AS total_obitos, "
        f"SUM(valor_total) AS valor_total "
        f"FROM insights_sih_hospital_mensal {where} "
        f"GROUP BY cnes ORDER BY total_aihs DESC LIMIT ?"
    )
    rows = query(sql, tuple(params) + (limit,))
    for r in rows:
        a = r["total_aihs"] or 0
        o = r["total_obitos"] or 0
        r["letalidade"] = round(100.0 * o / a, 2) if a else 0.0
    return _envelope(
        "hospitais", rows,
        uf=uf, municipio_ibge=municipio_ibge,
        ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )


# ---------------------------------------------------------------------------
# /resumo
# ---------------------------------------------------------------------------

@router.get("/resumo", summary="Resumo agregado: serie + capitulos + uti + perfil")
def resumo(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
):
    serie = serie_mensal(uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim, municipio_ibge=municipio_ibge)
    caps = capitulos(uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim, municipio_ibge=municipio_ibge, agrupar_por_ano=False)
    uti = uti_serie_mensal(uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim, municipio_ibge=municipio_ibge)
    prf = perfil(uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim, municipio_ibge=municipio_ibge, sexo=None, agrupar_por_ano=False)
    total_aihs = sum(r["total_aihs"] or 0 for r in serie["dados"])
    total_obitos = sum(r["obitos"] or 0 for r in serie["dados"])
    total_valor = sum(r["valor_total"] or 0 for r in serie["dados"])
    return {
        "ok": True,
        "sistema": "sih",
        "indicador": "resumo",
        "uf": uf.upper() if uf else None,
        "municipio_ibge": municipio_ibge,
        "ano": ano,
        "ano_inicio": ano_inicio,
        "ano_fim": ano_fim,
        "total_aihs": total_aihs,
        "total_obitos": total_obitos,
        "valor_total": total_valor,
        "letalidade_geral": round(100.0 * total_obitos / total_aihs, 2) if total_aihs else 0.0,
        "dados": {
            "serie_mensal": serie["dados"],
            "capitulos": caps["dados"],
            "uti_serie_mensal": uti["dados"],
            "perfil": prf["dados"],
        },
        "metadata": {"fonte": FONTE, "atualizado_em": _now_iso()},
    }
