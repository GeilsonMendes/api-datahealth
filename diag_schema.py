"""Inspeciona schema das tabelas SIM no SQLite."""
import sqlite3, json
con = sqlite3.connect("data/indicadores.db")
con.row_factory = sqlite3.Row

print("=== Tabelas insights_sim_*: ===")
tabs = con.execute("""
    SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'insights_sim%'
    ORDER BY name
""").fetchall()
for (t,) in [(r["name"],) for r in tabs]:
    cols = con.execute(f"PRAGMA table_info({t})").fetchall()
    n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    print(f"\n{t} ({n:,} linhas):")
    for c in cols:
        print(f"  {c['name']}: {c['type']}")

print("\n=== Tabelas dados_sim*: ===")
tabs = con.execute("""
    SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'dados_sim%'
""").fetchall()
for (t,) in [(r["name"],) for r in tabs]:
    cols = con.execute(f"PRAGMA table_info({t})").fetchall()
    n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    print(f"\n{t} ({n:,} linhas):")
    for c in cols:
        print(f"  {c['name']}: {c['type']}")

con.close()
