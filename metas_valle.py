import requests
import pandas as pd
from io import StringIO
import psycopg2
from psycopg2.extras import execute_batch
import os


# =========================
# LINK CSV GOOGLE SHEETS
# =========================
SHEET_URL = "https://docs.google.com/spreadsheets/d/1oS7VTEOmhaq1hZnns9unXS8qBNJq8yves0dtdZtJUlk/export?format=csv"

# =========================
# BANCO
# =========================
DB_CONFIG = {
    "host": os.environ.get("DB_HOST"),
    "database": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASS"),
    "port": int(os.environ.get("DB_PORT", 5432)),
    "sslmode": "require"
}
  
print("Baixando planilha...")

response = requests.get(SHEET_URL, timeout=60)
response.raise_for_status()

# UTF8 automático (ESSA É A PARTE QUE RESOLVE O Ã)
df = pd.read_csv(StringIO(response.text))

print("Planilha carregada")

# =========================
# TRATAMENTO
# =========================
df.columns = df.columns.str.strip()

df["cooperativa"] = df["cooperativa"].astype(str).str.strip()
df["meta"] = pd.to_numeric(df["meta"], errors="coerce")
df["data"] = pd.to_datetime(df["data"], errors="coerce")

df = df.dropna(subset=["cooperativa", "meta", "data"])

print(f"{len(df)} registros válidos")

# =========================
# CONECTAR POSTGRES
# =========================
conn = psycopg2.connect(**DB_CONFIG)
conn.autocommit = False
cur = conn.cursor()

# cria tabela
cur.execute("""
CREATE TABLE IF NOT EXISTS meta_valle (
    id SERIAL PRIMARY KEY,
    cooperativa TEXT,
    meta NUMERIC,
    data DATE,
    status TEXT DEFAULT 'ATIVO'
);
""")
conn.commit()

# chave única
cur.execute("""
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uq_meta_valle_coop_data'
    ) THEN
        ALTER TABLE meta_valle
        ADD CONSTRAINT uq_meta_valle_coop_data UNIQUE (cooperativa, data);
    END IF;
END$$;
""")
conn.commit()

# insert/update
sql = """
INSERT INTO meta_valle (cooperativa, meta, data, status)
VALUES (%s, %s, %s, 'ATIVO')
ON CONFLICT (cooperativa, data)
DO UPDATE SET
    meta = EXCLUDED.meta,
    status = 'ATIVO';
"""

rows = [
    (r["cooperativa"], r["meta"], r["data"].date())
    for _, r in df.iterrows()
]

execute_batch(cur, sql, rows, page_size=500)
conn.commit()

print("Dados inseridos")

# =========================
# CORRIGIR ACENTOS ANTIGOS
# =========================
cur.execute("""
UPDATE meta_valle
SET cooperativa =
    convert_from(convert_to(cooperativa, 'LATIN1'), 'UTF8')
WHERE octet_length(cooperativa) > length(cooperativa);
""")
conn.commit()

print("Acentos corrigidos")

cur.close()
conn.close()

print("Finalizado com sucesso")
