from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query

from ..auth import require_api_key
from ..db import query

router = APIRouter(
    prefix="/datasus/sim",
    tags=["SIM - Sistema de Informacao sobre Mortalidade"],
    dependencies=[Depends(require_api_key)],
)

FONTE = "SIM/DATASUS"


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
        "sistema": "sim",
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
        "SELECT uf, ano, SUM(obitos_totais) AS total_obitos, COUNT(*) AS linhas "
        "FROM insights_sim_resumo_mensal "
        "GROUP BY uf, ano ORDER BY uf, ano"
    )
    try:
        rows = query(sql)
    except Exception as e:
        raise HTTPException(500, detail=f"Erro consultando partitions: {e}")
    return _envelope("partitions", rows)


# ---------------------------------------------------------------------------
# /obitos/serie-mensal
# ---------------------------------------------------------------------------

@router.get("/obitos/serie-mensal", summary="Serie mensal de obitos")
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
        f"SUM(obitos_totais) AS total_obitos, "
        f"SUM(obitos_infantis) AS obitos_infantis, "
        f"SUM(obitos_maternos) AS obitos_maternos, "
        f"SUM(obitos_causa_externa) AS obitos_causa_externa, "
        f"SUM(obitos_cardiovasc) AS obitos_cardiovasculares, "
        f"SUM(obitos_neoplasias) AS obitos_neoplasias, "
        f"SUM(obitos_respiratorias) AS obitos_respiratorias "
        f"FROM insights_sim_resumo_mensal {where} "
        f"GROUP BY ano, mes ORDER BY ano, mes"
    )
    rows = query(sql, tuple(params))
    return _envelope(
        "obitos.serie-mensal", rows,
        uf=uf, municipio_ibge=municipio_ibge,
        ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )


# ---------------------------------------------------------------------------
# /obitos/municipios
# ---------------------------------------------------------------------------

@router.get("/obitos/municipios", summary="Top municipios por obitos")
def municipios(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
    limit: int = Query(100, ge=1, le=5570),
    agrupar_por_ano: bool = Query(False),
):
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim, municipio_ibge)
    if agrupar_por_ano:
        sql = (
            f"SELECT uf, municipio_ibge, ano, SUM(obitos_totais) AS total_obitos "
            f"FROM insights_sim_resumo_mensal {where} "
            f"GROUP BY uf, municipio_ibge, ano ORDER BY total_obitos DESC LIMIT ?"
        )
    else:
        sql = (
            f"SELECT uf, municipio_ibge, SUM(obitos_totais) AS total_obitos "
            f"FROM insights_sim_resumo_mensal {where} "
            f"GROUP BY uf, municipio_ibge ORDER BY total_obitos DESC LIMIT ?"
        )
    rows = query(sql, tuple(params) + (limit,))
    return _envelope(
        "obitos.municipios", rows,
        uf=uf, municipio_ibge=municipio_ibge,
        ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )


# ---------------------------------------------------------------------------
# /obitos/causas (CID-10 3 caracteres)
# ---------------------------------------------------------------------------

@router.get("/obitos/causas", summary="Causas de obito (CID-10 3 caracteres)")
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
    # cid_3car nao tem coluna de capitulo; se filtrar por capitulo, usamos JOIN com mensal_capitulo? Nao: simplificamos
    # filtrando pelo prefixo da letra do CID-10 (Cap. IX = I, Cap. II = C/D, etc.).
    # Implementacao mais simples: ignora capitulo se nao tiver mapping local.
    extra = None  # capitulo nao filtra direto na tabela cid_3car
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim, municipio_ibge, extra=extra)

    # Filtro por capitulo via prefixo
    if cid10_capitulo:
        # Mapa rapido capitulo romano -> letras CID
        cap_map = {
            "I": ["A", "B"], "II": ["C", "D"], "III": ["D"], "IV": ["E"],
            "V": ["F"], "VI": ["G"], "VII": ["H"], "VIII": ["H"],
            "IX": ["I"], "X": ["J"], "XI": ["K"], "XII": ["L"],
            "XIII": ["M"], "XIV": ["N"], "XV": ["O"], "XVI": ["P"],
            "XVII": ["Q"], "XVIII": ["R"], "XIX": ["S", "T"],
            "XX": ["V", "W", "X", "Y"], "XXI": ["Z"], "XXII": ["U"],
        }
        prefixos = cap_map.get(cid10_capitulo.upper(), [])
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
        f"SUM(obitos) AS total_obitos "
        f"FROM insights_sim_cid_3car_mensal {where} "
        f"GROUP BY cid_3car{group_year} "
        f"ORDER BY total_obitos DESC LIMIT ?"
    )
    rows = query(sql, tuple(params) + (limit,))
    return _envelope(
        "obitos.causas", rows,
        uf=uf, municipio_ibge=municipio_ibge,
        ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )


# ---------------------------------------------------------------------------
# /obitos/causas/capitulos
# ---------------------------------------------------------------------------

@router.get("/obitos/causas/capitulos", summary="Obitos por capitulo CID-10")
def causas_capitulos(
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
        f"SUM(obitos) AS total_obitos "
        f"FROM insights_sim_capitulo_mensal {where} "
        f"GROUP BY cid_capitulo{group_year} "
        f"ORDER BY total_obitos DESC"
    )
    rows = query(sql, tuple(params))
    total_geral = sum(r["total_obitos"] or 0 for r in rows) or 1
    for r in rows:
        r["pct"] = round(100.0 * (r["total_obitos"] or 0) / total_geral, 2)
    return _envelope(
        "obitos.causas.capitulos", rows,
        uf=uf, municipio_ibge=municipio_ibge,
        ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )


# ---------------------------------------------------------------------------
# /obitos/perfil
# ---------------------------------------------------------------------------

@router.get("/obitos/perfil", summary="Perfil demografico (sexo, faixa etaria, raca)")
def perfil(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
    sexo: Optional[str] = Query(None, pattern="^(Masculino|Feminino|Ignorado)$"),
    agrupar_por_ano: bool = Query(False),
):
    extra = [("sexo", sexo)] if sexo else None
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim, municipio_ibge, extra=extra)
    group_year = ", ano" if agrupar_por_ano else ""
    select_year = ", ano" if agrupar_por_ano else ""
    sql = (
        f"SELECT sexo, faixa_etaria, raca{select_year}, "
        f"SUM(obitos) AS total_obitos "
        f"FROM insights_sim_demografico_mensal {where} "
        f"GROUP BY sexo, faixa_etaria, raca{group_year} "
        f"ORDER BY total_obitos DESC"
    )
    rows = query(sql, tuple(params))
    return _envelope(
        "obitos.perfil", rows,
        uf=uf, municipio_ibge=municipio_ibge,
        ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )


# ---------------------------------------------------------------------------
# /obitos/local
# ---------------------------------------------------------------------------

@router.get("/obitos/local", summary="Obitos por local de ocorrencia")
def obitos_local(
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
        f"SELECT local_ocorrencia{select_year}, "
        f"SUM(obitos) AS total_obitos "
        f"FROM insights_sim_local_mensal {where} "
        f"GROUP BY local_ocorrencia{group_year} "
        f"ORDER BY total_obitos DESC"
    )
    rows = query(sql, tuple(params))
    total_geral = sum(r["total_obitos"] or 0 for r in rows) or 1
    for r in rows:
        r["pct"] = round(100.0 * (r["total_obitos"] or 0) / total_geral, 2)
    return _envelope(
        "obitos.local", rows,
        uf=uf, municipio_ibge=municipio_ibge,
        ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )


# ---------------------------------------------------------------------------
# /obitos/resumo (agrega outros)
# ---------------------------------------------------------------------------

@router.get("/obitos/resumo", summary="Resumo agregado: serie + capitulos + perfil + top municipios")
def resumo(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
):
    serie = serie_mensal(uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim, municipio_ibge=municipio_ibge)
    caps = causas_capitulos(uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim, municipio_ibge=municipio_ibge, agrupar_por_ano=False)
    prf = perfil(uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim, municipio_ibge=municipio_ibge, sexo=None, agrupar_por_ano=False)
    munis = municipios(uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim, municipio_ibge=municipio_ibge, limit=20, agrupar_por_ano=False)
    total_geral = sum(r["total_obitos"] or 0 for r in serie["dados"])
    return {
        "ok": True,
        "sistema": "sim",
        "indicador": "obitos.resumo",
        "uf": uf.upper() if uf else None,
        "municipio_ibge": municipio_ibge,
        "ano": ano,
        "ano_inicio": ano_inicio,
        "ano_fim": ano_fim,
        "total_obitos": total_geral,
        "dados": {
            "serie_mensal": serie["dados"],
            "capitulos": caps["dados"],
            "perfil": prf["dados"],
            "top_municipios": munis["dados"],
        },
        "metadata": {"fonte": FONTE, "atualizado_em": _now_iso()},
    }
