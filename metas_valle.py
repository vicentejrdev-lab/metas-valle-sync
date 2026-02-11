import pandas as pd
import psycopg2
from io import StringIO
import requests
import os

# =========================================================
# CONFIGURAÇÕES (GitHub Secrets)
# =========================================================
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASS")  # <<< NOME CORRETO DO SECRET

# Link do Google Sheets CSV
SHEET_URL = "https://docs.google.com/spreadsheets/d/1oS7VTEOmhaq1hZnns9unXS8qBNJq8yves0dtdZtJUlk/export?format=csv"

print("Baixando planilha...")
response = requests.get(SHEET_URL, timeout=30)
response.raise_for_status()

df = pd.read_csv(StringIO(response.text))
print("Planilha carregada")

# =========================================================
# NORMALIZAR COLUNAS
# =========================================================
df.columns = (
    df.columns
    .str.strip()
    .str.upper()
)

required_cols = {"ID", "COOPERATIVA", "META", "DATA", "STATUS"}
missing = required_cols - set(df.columns)

if missing:
    raise ValueError(f"Colunas obrigatórias ausentes na planilha: {missing}")

df = df.dropna(subset=["ID", "COOPERATIVA"])

# converter DATA para formato date do postgres
df["DATA"] = pd.to_datetime(df["DATA"], errors="coerce").dt.date

print(f"{len(df)} registros válidos")

# =========================================================
# CONEXÃO POSTGRESQL
# =========================================================
conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    connect_timeout=10,
    sslmode="disable"
)

cursor = conn.cursor()

# =========================================================
# GARANTIR TABELA
# =========================================================
cursor.execute("""
CREATE TABLE IF NOT EXISTS META_VALLE (
    ID INT PRIMARY KEY,
    COOPERATIVA VARCHAR(100) NOT NULL,
    META INT NOT NULL DEFAULT 0,
    DATA DATE,
    STATUS VARCHAR(100)
);
""")
conn.commit()

# =========================================================
# UPSERT
# =========================================================
sql = """
INSERT INTO META_VALLE (id, cooperativa, meta, data, status)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (id)
DO UPDATE SET
    cooperativa = EXCLUDED.cooperativa,
    meta = EXCLUDED.meta,
    data = EXCLUDED.data,
    status = EXCLUDED.status;
"""

print("Enviando dados ao banco...")

for _, row in df.iterrows():
    cursor.execute(
        sql,
        (
            int(row["ID"]),
            str(row["COOPERATIVA"]),
            int(row["META"]) if not pd.isna(row["META"]) else 0,
            row["DATA"],
            row["STATUS"] if not pd.isna(row["STATUS"]) else None
        )
    )

# =========================================================
# FINALIZAR
# =========================================================
conn.commit()
cursor.close()
conn.close()

print("Sincronização concluída com sucesso")
