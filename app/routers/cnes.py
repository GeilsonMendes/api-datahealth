from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query

from ..auth import require_api_key
from ..db import query

router = APIRouter(
    prefix="/datasus/cnes",
    tags=["CNES - Cadastro Nacional Estabelecimentos de Saude"],
    dependencies=[Depends(require_api_key)],
)

FONTE = "CNES/SUS"


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
        "sistema": "cnes",
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


# ---------------------------------------------------------------------------
# /partitions
# ---------------------------------------------------------------------------

@router.get("/partitions", summary="Listar particoes UF x Ano disponiveis")
def partitions():
    sql = (
        "SELECT uf, ano, SUM(total_estab) AS total_estab "
        "FROM cnes_estab_mensal "
        "GROUP BY uf, ano ORDER BY uf, ano"
    )
    try:
        rows = query(sql)
    except Exception as e:
        raise HTTPException(500, detail=f"Erro consultando partitions: {e}")
    return _envelope("partitions", rows)


# ---------------------------------------------------------------------------
# /estabelecimentos
# ---------------------------------------------------------------------------

@router.get("/estabelecimentos", summary="Serie temporal de estabelecimentos")
def estabelecimentos(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
    tipo_unidade: Optional[str] = Query(None),
    agrupar_por_tipo: bool = Query(False),
):
    extra = [("tipo_unidade", tipo_unidade)] if tipo_unidade else None
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim, municipio_ibge, extra=extra)

    sql_serie = (
        f"SELECT ano, mes, SUM(total_estab) AS total_estab "
        f"FROM cnes_estab_mensal {where} "
        f"GROUP BY ano, mes ORDER BY ano, mes"
    )
    serie = query(sql_serie, tuple(params))

    dados: Dict[str, Any] = {"serie": serie}

    if agrupar_por_tipo:
        sql_tipos = (
            f"SELECT tipo_unidade, SUM(total_estab) AS total_estab "
            f"FROM cnes_estab_mensal {where} "
            f"GROUP BY tipo_unidade ORDER BY total_estab DESC LIMIT 20"
        )
        dados["tipos"] = query(sql_tipos, tuple(params))

    return _envelope(
        "estabelecimentos", dados,
        uf=uf, municipio_ibge=municipio_ibge,
        ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )


# ---------------------------------------------------------------------------
# /leitos
# ---------------------------------------------------------------------------

@router.get("/leitos", summary="Serie temporal de leitos hospitalares")
def leitos(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
    especialidade: Optional[str] = Query(None),
):
    extra = [("especialidade", especialidade)] if especialidade else None
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim, municipio_ibge, extra=extra)

    sql_serie = (
        f"SELECT ano, mes, SUM(qt_existente) AS qt_existente, SUM(qt_sus) AS qt_sus "
        f"FROM cnes_leitos_mensal {where} "
        f"GROUP BY ano, mes ORDER BY ano, mes"
    )
    serie = query(sql_serie, tuple(params))

    sql_top = (
        f"SELECT especialidade, SUM(qt_existente) AS qt_existente, SUM(qt_sus) AS qt_sus "
        f"FROM cnes_leitos_mensal {where} "
        f"GROUP BY especialidade ORDER BY qt_existente DESC LIMIT 10"
    )
    top = query(sql_top, tuple(params))

    return _envelope(
        "leitos", {"serie": serie, "top_especialidades": top},
        uf=uf, municipio_ibge=municipio_ibge,
        ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )


# ---------------------------------------------------------------------------
# /equipamentos
# ---------------------------------------------------------------------------

@router.get("/equipamentos", summary="Serie temporal de equipamentos")
def equipamentos(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
    tipo_equipamento: Optional[str] = Query(None),
):
    extra = [("tipo_equipamento", tipo_equipamento)] if tipo_equipamento else None
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim, municipio_ibge, extra=extra)

    sql_serie = (
        f"SELECT ano, mes, SUM(qt_existente) AS qt_existente, SUM(qt_em_uso) AS qt_em_uso "
        f"FROM cnes_equip_mensal {where} "
        f"GROUP BY ano, mes ORDER BY ano, mes"
    )
    serie = query(sql_serie, tuple(params))

    sql_top = (
        f"SELECT tipo_equipamento, SUM(qt_existente) AS qt_existente, SUM(qt_em_uso) AS qt_em_uso "
        f"FROM cnes_equip_mensal {where} "
        f"GROUP BY tipo_equipamento ORDER BY qt_existente DESC LIMIT 10"
    )
    top = query(sql_top, tuple(params))

    return _envelope(
        "equipamentos", {"serie": serie, "top_tipos": top},
        uf=uf, municipio_ibge=municipio_ibge,
        ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )


# ---------------------------------------------------------------------------
# /profissionais
# ---------------------------------------------------------------------------

@router.get("/profissionais", summary="Serie temporal de profissionais e vinculos")
def profissionais(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
    cbo_grupo: Optional[str] = Query(None),
):
    extra = [("cbo_grupo", cbo_grupo)] if cbo_grupo else None
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim, municipio_ibge, extra=extra)

    sql_serie = (
        f"SELECT ano, mes, "
        f"SUM(total_profissionais) AS total_profissionais, "
        f"SUM(total_vinculos) AS total_vinculos "
        f"FROM cnes_prof_mensal {where} "
        f"GROUP BY ano, mes ORDER BY ano, mes"
    )
    serie = query(sql_serie, tuple(params))

    sql_top = (
        f"SELECT cbo_grupo, cbo_grupo_nome, "
        f"SUM(total_profissionais) AS total_profissionais, "
        f"SUM(total_vinculos) AS total_vinculos "
        f"FROM cnes_prof_mensal {where} "
        f"GROUP BY cbo_grupo, cbo_grupo_nome "
        f"ORDER BY total_profissionais DESC LIMIT 10"
    )
    top = query(sql_top, tuple(params))

    return _envelope(
        "profissionais", {"serie": serie, "top_cbo": top},
        uf=uf, municipio_ibge=municipio_ibge,
        ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )


# ---------------------------------------------------------------------------
# /equipes
# ---------------------------------------------------------------------------

@router.get("/equipes", summary="Serie temporal de equipes")
def equipes(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
    tipo_equipe: Optional[str] = Query(None),
):
    extra = [("tipo_equipe", tipo_equipe)] if tipo_equipe else None
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim, municipio_ibge, extra=extra)

    sql_serie = (
        f"SELECT ano, mes, SUM(total_equipes) AS total_equipes "
        f"FROM cnes_equipes_mensal {where} "
        f"GROUP BY ano, mes ORDER BY ano, mes"
    )
    serie = query(sql_serie, tuple(params))

    sql_top = (
        f"SELECT tipo_equipe, SUM(total_equipes) AS total_equipes "
        f"FROM cnes_equipes_mensal {where} "
        f"GROUP BY tipo_equipe ORDER BY total_equipes DESC LIMIT 20"
    )
    top = query(sql_top, tuple(params))

    return _envelope(
        "equipes", {"serie": serie, "top_tipos": top},
        uf=uf, municipio_ibge=municipio_ibge,
        ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )


# ---------------------------------------------------------------------------
# /lookup
# ---------------------------------------------------------------------------

@router.get("/lookup", summary="Buscar estabelecimentos (cadastro CNES)")
def lookup(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
    q: Optional[str] = Query(None, description="Busca por nome ou CNES"),
    limit: int = Query(50, ge=1, le=500),
):
    clauses: List[str] = ["1=1"]
    params: List[Any] = []
    if uf:
        clauses.append("uf = ?")
        params.append(uf.upper())
    if municipio_ibge:
        clauses.append("(municipio_ibge = ? OR SUBSTR(municipio_ibge,1,6) = SUBSTR(?,1,6))")
        params.extend([municipio_ibge, municipio_ibge])
    if q:
        like = f"%{q}%"
        clauses.append("(UPPER(nome) LIKE UPPER(?) OR cnes LIKE ?)")
        params.extend([like, like])

    where = "WHERE " + " AND ".join(clauses)
    sql = (
        f"SELECT cnes, nome, municipio_ibge, municipio_nome, uf, tipo_unidade "
        f"FROM cnes_lookup {where} ORDER BY nome LIMIT ?"
    )
    rows = query(sql, tuple(params) + (limit,))
    return _envelope("lookup", rows, uf=uf, municipio_ibge=municipio_ibge)


# ---------------------------------------------------------------------------
# /resumo
# ---------------------------------------------------------------------------

@router.get("/resumo", summary="Resumo agregado: estabelecimentos, leitos, equipamentos, profissionais, equipes")
def resumo(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    municipio_ibge: Optional[str] = Query(None, min_length=6, max_length=7),
    ano: Optional[int] = Query(None, ge=2000, le=2030, description="Se omitido, usa ultimo ano disponivel"),
):
    # Determina ano de referencia
    if ano is None:
        where_y, params_y = _build_filters(uf, None, None, None, municipio_ibge)
        sql_y = f"SELECT MAX(ano) AS ano FROM cnes_estab_mensal {where_y}"
        rows_y = query(sql_y, tuple(params_y))
        ano = rows_y[0]["ano"] if rows_y and rows_y[0].get("ano") else None

    if ano is None:
        return _envelope(
            "resumo",
            {
                "total_estabelecimentos": 0,
                "total_leitos_existentes": 0,
                "total_leitos_sus": 0,
                "total_equipamentos": 0,
                "total_profissionais": 0,
                "total_vinculos": 0,
                "total_equipes": 0,
            },
            uf=uf, municipio_ibge=municipio_ibge,
        )

    where, params = _build_filters(uf, ano, None, None, municipio_ibge)

    def _scalar(sql: str) -> int:
        r = query(sql, tuple(params))
        if not r:
            return 0
        v = list(r[0].values())[0]
        return int(v) if v is not None else 0

    # No CNES, mes mais recente do ano (snapshot)
    sql_mes_estab = f"SELECT MAX(mes) AS mes FROM cnes_estab_mensal {where}"
    rows_mes = query(sql_mes_estab, tuple(params))
    mes_ref = rows_mes[0]["mes"] if rows_mes and rows_mes[0].get("mes") else None

    if mes_ref is not None:
        where_snap = where + " AND mes = ?"
        params_snap = params + [mes_ref]
    else:
        where_snap = where
        params_snap = params

    def _scalar_snap(sql: str) -> int:
        r = query(sql, tuple(params_snap))
        if not r:
            return 0
        v = list(r[0].values())[0]
        return int(v) if v is not None else 0

    total_estab = _scalar_snap(f"SELECT SUM(total_estab) FROM cnes_estab_mensal {where_snap}")
    total_leitos_exist = _scalar_snap(f"SELECT SUM(qt_existente) FROM cnes_leitos_mensal {where_snap}")
    total_leitos_sus = _scalar_snap(f"SELECT SUM(qt_sus) FROM cnes_leitos_mensal {where_snap}")
    total_equip = _scalar_snap(f"SELECT SUM(qt_existente) FROM cnes_equip_mensal {where_snap}")
    total_prof = _scalar_snap(f"SELECT SUM(total_profissionais) FROM cnes_prof_mensal {where_snap}")
    total_vinc = _scalar_snap(f"SELECT SUM(total_vinculos) FROM cnes_prof_mensal {where_snap}")
    total_equipes = _scalar_snap(f"SELECT SUM(total_equipes) FROM cnes_equipes_mensal {where_snap}")

    return _envelope(
        "resumo",
        {
            "ano_referencia": ano,
            "mes_referencia": mes_ref,
            "total_estabelecimentos": total_estab,
            "total_leitos_existentes": total_leitos_exist,
            "total_leitos_sus": total_leitos_sus,
            "total_equipamentos": total_equip,
            "total_profissionais": total_prof,
            "total_vinculos": total_vinc,
            "total_equipes": total_equipes,
        },
        uf=uf, municipio_ibge=municipio_ibge, ano=ano,
    )
