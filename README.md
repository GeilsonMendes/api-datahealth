# api-datahealth

API REST em **Python FastAPI** que replica o padrao da `api-datasus.pluralmed.com.br`.

- Mesma estrutura de endpoints (`/datasus/<sistema>/<indicador>`)
- Autenticacao via header `X-API-Key`
- Resposta JSON padronizada (`ok`, `sistema`, `indicador`, `dados`, `total`, `metadata`)
- Swagger UI em `/docs`, ReDoc em `/redoc`
- SQLite read-only servindo as tabelas `gold` produzidas em `indicadores-politicas`

Status atual: **bootstrap + modulo SIM (Mortalidade)**. Proximos: SIH, SINASC, SIA, CNES, PNI, SINAN, Oncologia, Indicadores Cruzados.

---

## Setup local (primeira vez)

### 1. Copiar o banco de dados

A API le os mesmos dados gold gerados pelo projeto `indicadores-politicas`.
A pasta `data/` esta no `.gitignore` (banco eh grande), entao voce precisa copiar manualmente.

PowerShell:

```powershell
Copy-Item "..\indicadores-politicas\data\indicadores.db" ".\data\indicadores.db"
```

> Recomendado: copie tambem `indicadores.db-wal` e `indicadores.db-shm` se existirem.
> Nao precisa copiar `dw.duckdb` (a API usa apenas SQLite).

### 2. Subir com Docker (recomendado)

```powershell
docker-compose up --build
```

A primeira build instala as dependencias Python (1-2 min). Depois disso:

- API: http://localhost:8000
- Docs (Swagger): http://localhost:8000/docs
- Status do DB: http://localhost:8000/datasus/status

API key padrao em dev: `dev-key-12345`. Use o botao **Authorize** no Swagger e cole essa chave em `X-API-Key`.

### 3. Alternativa: rodar sem Docker

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
uvicorn app.main:app --reload --port 8000
```

---

## Endpoints disponiveis (modulo SIM)

Todos exigem header `X-API-Key`. Parametros sao opcionais salvo indicacao.

| Metodo | Caminho | Descricao |
|---|---|---|
| GET | `/health` | Healthcheck (publico) |
| GET | `/datasus/status` | Status do SQLite + tabelas SIM |
| GET | `/datasus/sim/partitions` | Lista UF x Ano disponiveis |
| GET | `/datasus/sim/obitos/serie-mensal` | Serie mensal de obitos |
| GET | `/datasus/sim/obitos/municipios` | Top municipios por obitos |
| GET | `/datasus/sim/obitos/causas` | Causas detalhadas (CID-10 3 caracteres) |
| GET | `/datasus/sim/obitos/causas/capitulos` | Obitos por capitulo CID-10 + % |
| GET | `/datasus/sim/obitos/perfil` | Perfil (sexo, faixa etaria, raca/cor) |
| GET | `/datasus/sim/obitos/resumo` | Agrega serie + capitulos + perfil + top municipios |

Filtros comuns: `uf`, `ano`, `ano_inicio`, `ano_fim`, `municipio_ibge`. Alguns aceitam `limit`, `agrupar_por_ano`, `cid10_capitulo`, `sexo`.

### Exemplo (PowerShell)

```powershell
$h = @{ "X-API-Key" = "dev-key-12345" }
Invoke-RestMethod "http://localhost:8000/datasus/sim/obitos/serie-mensal?uf=CE&ano_inicio=2020&ano_fim=2024" -Headers $h | ConvertTo-Json -Depth 5
```

---

## Testes

```powershell
pip install -r requirements.txt
pytest -v
```

Os testes que dependem do SQLite sao automaticamente pulados se `data/indicadores.db` nao existir.

---

## Criando API keys de producao

`API_KEYS` e uma string com chaves separadas por virgula. Geramos assim:

```powershell
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

Coloque no `.env` (local) ou nas variaveis de ambiente do Railway:

```
API_KEYS=chave-cliente-A-xxx,chave-cliente-B-yyy
```

---

## Deploy no Railway

1. Crie um projeto no Railway apontando para este repo (ou pasta).
2. Railway detecta `Dockerfile` + `railway.json` automaticamente.
3. Configure variaveis de ambiente:
   - `API_KEYS` (lista separada por virgula)
   - `CORS_ORIGINS` (dominios autorizados)
   - `SQLITE_PATH=/data/indicadores.db`
4. Anexe um **Volume** persistente em `/data` e faca upload do `indicadores.db` (use `railway run` ou um job pre-deploy).
5. Healthcheck em `/health` ja configurado em `railway.json`.

---

## Atualizando os dados

Os dados gold vem do pipeline em `indicadores-politicas`. Fluxo:

1. No projeto `indicadores-politicas`, rodar os scripts ETL (bronze -> silver -> gold).
2. Copiar `indicadores.db` atualizado para `api-datahealth/data/`.
3. Reiniciar o container (`docker-compose restart` ou redeploy no Railway com novo volume).

---

## Estrutura de pastas

```
api-datahealth/
  app/
    main.py            FastAPI app, CORS, handlers, routers
    auth.py            X-API-Key dependency
    config.py          pydantic-settings
    db.py              sqlite3 read-only (uri mode)
    exceptions.py      handlers padronizados
    routers/
      system.py        /health, /datasus/status
      sim.py           7 endpoints SIM
    schemas/
      base.py          RespostaPadrao
      sim.py           modelos pydantic SIM
  tests/test_sim.py    pytest + TestClient
  data/                gitignored — cole indicadores.db aqui
  Dockerfile           multi-stage Python 3.11
  docker-compose.yml   dev local
  railway.json         deploy Railway
  requirements.txt
  pyproject.toml
  .env.example
```
