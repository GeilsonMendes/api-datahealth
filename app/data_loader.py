from __future__ import annotations
import gzip, urllib.request, sys
from pathlib import Path

# URL do GitHub Release (atualizar quando publicar)
DATA_URL = "https://github.com/GeilsonMendes/api-datahealth/releases/download/v0.1.0-data-CE/indicadores.db.gz"

def ensure_database(path: Path) -> bool:
    """Baixa e descompacta o SQLite se nao existir no volume.
    Retorna True se baixou, False se ja existia."""
    if path.exists() and path.stat().st_size > 1_000_000:
        print(f"[data_loader] DB ja existe: {path} ({path.stat().st_size/1024/1024:.1f} MB)", flush=True)
        return False

    path.parent.mkdir(parents=True, exist_ok=True)
    gz_path = path.with_suffix(path.suffix + ".gz")

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
        print(f"[data_loader] DB pronto: {path} ({size:.1f} MB)", flush=True)
        return True
    except Exception as e:
        print(f"[data_loader] ERRO baixando dados: {e}", file=sys.stderr, flush=True)
        if gz_path.exists():
            gz_path.unlink()
        # Nao crasha — endpoints retornarao "tabela nao encontrada" graciosamente
        return False
