"""Testes basicos dos endpoints SIM.

Pre-requisito: ./data/indicadores.db deve existir e conter as tabelas
insights_sim_*. Caso contrario, os testes que dependem de DB sao pulados.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("API_KEYS", "test-key-1")
os.environ.setdefault("SQLITE_PATH", "./data/indicadores.db")

from app.main import app  # noqa: E402
from app.config import get_settings  # noqa: E402

client = TestClient(app)
HEADERS = {"X-API-Key": "test-key-1"}


def _db_available() -> bool:
    return get_settings().sqlite_abs_path.exists()


db_required = pytest.mark.skipif(not _db_available(), reason="indicadores.db nao encontrado")


def test_health_publico():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["ok"] is True


def test_root_publico():
    r = client.get("/")
    assert r.status_code == 200
    assert "sim" in r.json()["modulos_disponiveis"]


def test_sim_sem_api_key_401():
    r = client.get("/datasus/sim/partitions")
    assert r.status_code == 401


def test_sim_api_key_invalida_403():
    r = client.get("/datasus/sim/partitions", headers={"X-API-Key": "errada"})
    assert r.status_code == 403


@db_required
def test_partitions():
    r = client.get("/datasus/sim/partitions", headers=HEADERS)
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["sistema"] == "sim"
    assert isinstance(body["dados"], list)


@db_required
def test_serie_mensal_uf():
    r = client.get("/datasus/sim/obitos/serie-mensal?uf=CE&ano_inicio=2020&ano_fim=2022", headers=HEADERS)
    assert r.status_code == 200
    body = r.json()
    assert body["uf"] == "CE"
    for row in body["dados"]:
        assert "ano" in row and "mes" in row and "total_obitos" in row


@db_required
def test_municipios_top():
    r = client.get("/datasus/sim/obitos/municipios?uf=CE&ano=2022&limit=10", headers=HEADERS)
    assert r.status_code == 200
    body = r.json()
    assert len(body["dados"]) <= 10


@db_required
def test_causas():
    r = client.get("/datasus/sim/obitos/causas?uf=CE&ano=2022&limit=20", headers=HEADERS)
    assert r.status_code == 200
    assert r.json()["ok"] is True


@db_required
def test_capitulos_pct():
    r = client.get("/datasus/sim/obitos/causas/capitulos?uf=CE&ano=2022", headers=HEADERS)
    assert r.status_code == 200
    body = r.json()
    if body["dados"]:
        assert "pct" in body["dados"][0]


@db_required
def test_perfil():
    r = client.get("/datasus/sim/obitos/perfil?uf=CE&ano=2022", headers=HEADERS)
    assert r.status_code == 200


@db_required
def test_resumo():
    r = client.get("/datasus/sim/obitos/resumo?uf=CE&ano=2022", headers=HEADERS)
    assert r.status_code == 200
    body = r.json()
    assert "serie_mensal" in body["dados"]
    assert "capitulos" in body["dados"]
    assert "perfil" in body["dados"]
    assert "top_municipios" in body["dados"]
