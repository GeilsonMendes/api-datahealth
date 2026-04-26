from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query

from ..auth import require_api_key
from ..db import query

router = APIRouter(
    prefix="/datasus/sia",
    tags=["SIA - Producao Ambulatorial SUS"],
    dependencies=[Depends(require_api_key)],
)

FONTE = "SIA/SUS"


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _build_filters(
    uf: Optional[str],
    ano: Optional[int],
    ano_inicio: Optional[int],
    ano_fim: Optional[int],
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
    ano: Optional[int] = None,
    ano_inicio: Optional[int] = None,
    ano_fim: Optional[int] = None,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "ok": True,
        "sistema": "sia",
        "indicador": indicador,
    }
    if uf:
        out["uf"] = uf.upper()
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
        "SELECT uf, ano, SUM(qtd_aprovada) AS total_qtd_aprovada, COUNT(*) AS linhas "
        "FROM insights_sia_resumo_mensal_uf "
        "GROUP BY uf, ano ORDER BY uf, ano"
    )
    try:
        rows = query(sql)
    except Exception as e:
        raise HTTPException(500, detail=f"Erro consultando partitions: {e}")
    return _envelope("partitions", rows)


# ---------------------------------------------------------------------------
# /producao/serie
# ---------------------------------------------------------------------------

@router.get("/producao/serie", summary="Serie mensal de producao ambulatorial")
def producao_serie(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
):
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim)
    sql = (
        f"SELECT ano, mes, "
        f"SUM(qtd_aprovada) AS qtd_aprovada, "
        f"SUM(valor_aprovado) AS valor_aprovado, "
        f"SUM(valor_alta_complex) AS valor_alta_complex, "
        f"SUM(valor_media_complex) AS valor_media_complex, "
        f"SUM(valor_sem_complex) AS valor_sem_complex, "
        f"SUM(proc_distintos) AS proc_distintos, "
        f"SUM(cnes_distintos) AS cnes_distintos "
        f"FROM insights_sia_resumo_mensal_uf {where} "
        f"GROUP BY ano, mes ORDER BY ano, mes"
    )
    rows = query(sql, tuple(params))
    return _envelope(
        "producao.serie", rows,
        uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )


# ---------------------------------------------------------------------------
# /grupos
# ---------------------------------------------------------------------------

@router.get("/grupos", summary="Producao por grupo SIGTAP")
def grupos(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
):
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim)
    sql = (
        f"SELECT grupo_sigtap, "
        f"SUM(qtd_aprovada) AS qtd_aprovada, "
        f"SUM(valor_aprovado) AS valor_aprovado "
        f"FROM insights_sia_grupo_anual_uf {where} "
        f"GROUP BY grupo_sigtap ORDER BY valor_aprovado DESC"
    )
    rows = query(sql, tuple(params))
    total_valor = sum((r["valor_aprovado"] or 0) for r in rows) or 1
    for r in rows:
        r["pct"] = round(100.0 * (r["valor_aprovado"] or 0) / total_valor, 2)
    return _envelope(
        "grupos", rows,
        uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )


# ---------------------------------------------------------------------------
# /procedimentos
# ---------------------------------------------------------------------------

@router.get("/procedimentos", summary="Top procedimentos SIGTAP")
def procedimentos(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    limit: int = Query(50, ge=1, le=5000),
):
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim)
    sql = (
        f"SELECT pa_proc_id, "
        f"SUM(qtd_aprovada) AS qtd_aprovada, "
        f"SUM(valor_aprovado) AS valor_aprovado "
        f"FROM insights_sia_proc_anual_uf {where} "
        f"GROUP BY pa_proc_id ORDER BY valor_aprovado DESC LIMIT ?"
    )
    rows = query(sql, tuple(params) + (limit,))
    return _envelope(
        "procedimentos", rows,
        uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )


# ---------------------------------------------------------------------------
# /complexidade
# ---------------------------------------------------------------------------

@router.get("/complexidade", summary="Producao por nivel de complexidade")
def complexidade(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
):
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim)
    sql = (
        f"SELECT nivel_complexidade, "
        f"SUM(qtd_aprovada) AS qtd_aprovada, "
        f"SUM(valor_aprovado) AS valor_aprovado "
        f"FROM insights_sia_complexidade_anual_uf {where} "
        f"GROUP BY nivel_complexidade ORDER BY valor_aprovado DESC"
    )
    rows = query(sql, tuple(params))
    total_valor = sum((r["valor_aprovado"] or 0) for r in rows) or 1
    for r in rows:
        r["pct"] = round(100.0 * (r["valor_aprovado"] or 0) / total_valor, 2)
    return _envelope(
        "complexidade", rows,
        uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )


# ---------------------------------------------------------------------------
# /cnes
# ---------------------------------------------------------------------------

@router.get("/cnes", summary="Top estabelecimentos CNES por producao")
def cnes_top(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    limit: int = Query(100, ge=1, le=5000),
):
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim)
    sql = (
        f"SELECT cnes, "
        f"SUM(qtd_aprovada) AS qtd_aprovada, "
        f"SUM(valor_aprovado) AS valor_aprovado, "
        f"SUM(proc_distintos) AS proc_distintos "
        f"FROM insights_sia_cnes_anual_uf {where} "
        f"GROUP BY cnes ORDER BY valor_aprovado DESC LIMIT ?"
    )
    rows = query(sql, tuple(params) + (limit,))
    return _envelope(
        "cnes", rows,
        uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )


# ---------------------------------------------------------------------------
# /perfil
# ---------------------------------------------------------------------------

@router.get("/perfil", summary="Perfil demografico (sexo, faixa etaria)")
def perfil(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
    sexo: Optional[str] = Query(None),
):
    extra = [("sexo", sexo)] if sexo else None
    where, params = _build_filters(uf, ano, ano_inicio, ano_fim, extra=extra)
    sql = (
        f"SELECT sexo, faixa_etaria, "
        f"SUM(qtd_aprovada) AS qtd_aprovada, "
        f"SUM(valor_aprovado) AS valor_aprovado "
        f"FROM insights_sia_demografico_anual_uf {where} "
        f"GROUP BY sexo, faixa_etaria ORDER BY qtd_aprovada DESC"
    )
    rows = query(sql, tuple(params))
    return _envelope(
        "perfil", rows,
        uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim,
    )


# ---------------------------------------------------------------------------
# /resumo
# ---------------------------------------------------------------------------

@router.get("/resumo", summary="Resumo agregado: serie + grupos + complexidade + perfil")
def resumo(
    uf: Optional[str] = Query(None, min_length=2, max_length=2),
    ano: Optional[int] = Query(None, ge=2000, le=2030),
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2030),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2030),
):
    serie = producao_serie(uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim)
    grp = grupos(uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim)
    cmplx = complexidade(uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim)
    prf = perfil(uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim, sexo=None)
    total_qtd = sum((r["qtd_aprovada"] or 0) for r in serie["dados"])
    total_valor = sum((r["valor_aprovado"] or 0) for r in serie["dados"])
    return {
        "ok": True,
        "sistema": "sia",
        "indicador": "resumo",
        "uf": uf.upper() if uf else None,
        "ano": ano,
        "ano_inicio": ano_inicio,
        "ano_fim": ano_fim,
        "total_qtd_aprovada": total_qtd,
        "total_valor_aprovado": total_valor,
        "dados": {
            "serie_mensal": serie["dados"],
            "grupos": grp["dados"],
            "complexidade": cmplx["dados"],
            "perfil": prf["dados"],
        },
        "metadata": {"fonte": FONTE, "atualizado_em": _now_iso()},
    }
