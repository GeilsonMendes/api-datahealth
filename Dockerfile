# syntax=docker/dockerfile:1.6
FROM python:3.11-slim AS builder

ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /build
COPY requirements.txt .
RUN pip install --prefix=/install -r requirements.txt

# ---- runtime ----
FROM python:3.11-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH=/usr/local/bin:$PATH

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /code
COPY --from=builder /install /usr/local
COPY app ./app

ENV SQLITE_PATH=/data/indicadores.db
ENV PORT=8000
EXPOSE 8000

# Use shell form to expand $PORT (Railway define dinamicamente)
CMD uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000} --workers 2
