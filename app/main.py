from __future__ import annotations

from fastapi import FastAPI
from fastapi.exceptions import HTTPException
from fastapi.middleware.cors import CORSMiddleware

from .config import get_settings
from .exceptions import generic_exception_handler, http_exception_handler
from .routers import cnes, indicadores, oncologia, pni, sim, sih, sinan, sinasc, system

settings = get_settings()

app = FastAPI(
    title="api-datahealth",
    description=(
        "API REST para indicadores de saude publica (DATASUS).\n\n"
        "Replica o padrao de api-datasus.pluralmed.com.br.\n"
        "Autenticacao via header `X-API-Key`."
    ),
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_exception_handler(HTTPException, http_exception_handler)
app.add_exception_handler(Exception, generic_exception_handler)

app.include_router(system.router)
app.include_router(sim.router)
app.include_router(sih.router)
app.include_router(sinasc.router)
app.include_router(cnes.router)
app.include_router(pni.router)
app.include_router(oncologia.router)
app.include_router(sinan.router)
app.include_router(indicadores.router)


@app.get("/", tags=["system"])
def root():
    return {
        "ok": True,
        "servico": "api-datahealth",
        "versao": "0.1.0",
        "docs": "/docs",
        "modulos_disponiveis": ["sim", "sih", "sinasc", "cnes", "pni", "oncologia", "sinan", "indicadores"],
        "modulos_proximos": ["sia"],
    }
