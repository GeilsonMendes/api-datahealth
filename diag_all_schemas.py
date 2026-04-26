"""Inspeciona schemas de todas as tabelas gold para gerar reference para agentes."""
import sqlite3
con = sqlite3.connect("data/indicadores.db")

prefixos = [
    "insights_sih", "dados_sih",
    "insights_sinasc", "dados_sinasc",
    "insights_sia", "dados_sia",
    "cnes_",
    "pni_",
    "oncologia_",
    "insights_sinan", "sinan_mensal",
    "indicador_tmi", "indicador_rmm",
    "populacao", "territorios",
    "indicadores", "politicas",
]

for prefixo in prefixos:
    tabs = con.execute(f"""
        SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '{prefixo}%'
        ORDER BY name
    """).fetchall()
    if not tabs:
        continue
    for (t,) in tabs:
        cols = con.execute(f"PRAGMA table_info({t})").fetchall()
        try:
            n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        except Exception:
            n = "?"
        print(f"\n=== {t} ({n:,} linhas) ===" if isinstance(n, int) else f"\n=== {t} (? linhas) ===")
        for c in cols:
            print(f"  {c[1]}: {c[2]}")
con.close()
