from __future__ import annotations
import gzip, urllib.request, sys, sqlite3
from pathlib import Path

# URL do GitHub Release
DATA_URL = "https://github.com/GeilsonMendes/api-datahealth/releases/download/v0.1.0-data-CE/indicadores.db.gz"

# Tamanho minimo razoavel do DB descompactado (em bytes). Menor que isso = corrompido/parcial.
MIN_DB_SIZE = 100_000_000  # 100 MB (real ~1.14 GB)


def _is_valid_sqlite(path: Path) -> bool:
    """Verifica se arquivo abre como SQLite e tem tabelas gold."""
    try:
        con = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
        try:
            row = con.execute(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='insights_sim_resumo_mensal'"
            ).fetchone()
            return row and row[0] > 0
        finally:
            con.close()
    except Exception as e:
        print(f"[data_loader] DB invalido ({path}): {e}", flush=True)
        return False


def ensure_database(path: Path) -> bool:
    """Baixa e descompacta o SQLite se nao existir/for invalido no volume.
    Retorna True se baixou, False se ja existia (valido)."""
    # Se o arquivo existe e e SQLite valido, OK
    if path.exists() and path.stat().st_size > MIN_DB_SIZE and _is_valid_sqlite(path):
        print(f"[data_loader] DB OK: {path} ({path.stat().st_size/1024/1024:.1f} MB)", flush=True)
        return False

    # Arquivo existe mas e invalido/parcial — remover
    if path.exists():
        try:
            print(f"[data_loader] Removendo DB invalido/parcial: {path} ({path.stat().st_size/1024/1024:.1f} MB)", flush=True)
            path.unlink()
        except Exception as e:
            print(f"[data_loader] erro removendo {path}: {e}", file=sys.stderr, flush=True)

    path.parent.mkdir(parents=True, exist_ok=True)
    gz_path = path.with_suffix(path.suffix + ".gz")
    if gz_path.exists():
        try:
            gz_path.unlink()
        except Exception:
            pass

    try:
        print(f"[data_loader] Baixando {DATA_URL}...", flush=True)
        urllib.request.urlretrieve(DATA_URL, gz_path)
        gz_size = gz_path.stat().st_size / 1024 / 1024
        print(f"[data_loader] Baixado: {gz_size:.1f} MB. Descompactando...", flush=True)

        with gzip.open(gz_path, "rb") as gz, open(path, "wb") as out:
            while True:
                chunk = gz.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
        gz_path.unlink()
        size = path.stat().st_size / 1024 / 1024
        print(f"[data_loader] Descompactado: {size:.1f} MB. Validando...", flush=True)

        if not _is_valid_sqlite(path):
            print(f"[data_loader] ERRO: DB descompactado nao e SQLite valido", file=sys.stderr, flush=True)
            try:
                path.unlink()
            except Exception:
                pass
            return False

        print(f"[data_loader] DB pronto: {path} ({size:.1f} MB)", flush=True)
        return True
    except Exception as e:
        print(f"[data_loader] ERRO baixando dados: {e}", file=sys.stderr, flush=True)
        if gz_path.exists():
            try:
                gz_path.unlink()
            except Exception:
                pass
        return False
