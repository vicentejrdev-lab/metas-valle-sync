import pandas as pd
import psycopg2
from io import StringIO
import requests
import os
import time

# =========================================================
# CONFIGURAÇÕES (GitHub Secrets / Variáveis de Ambiente)
# =========================================================
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Link público do Google Sheets (CSV)
SHEET_URL = "https://docs.google.com/spreadsheets/d/1oS7VTEOmhaq1hZnns9unXS8qBNJq8yves0dtdZtJUlk/export?format=csv"

# =========================================================
# 1) LER A PLANILHA (COM QUEBRA DE CACHE)
# =========================================================
response = requests.get(
    f"{SHEET_URL}&_ts={int(time.time())}",  # cache-buster
    timeout=30
)
response.raise_for_status()

df = pd.read_csv(StringIO(response.text))

# =========================================================
# 2) NORMALIZAR NOMES DAS COLUNAS
# =========================================================
df.columns = (
    df.columns
      .str.strip()
      .str.upper()
)

# Validação mínima das colunas obrigatórias
required_cols = {"ID", "COOPERATIVA", "META", "DATA", "STATUS"}
missing = required_cols - set(df.columns)
if missing:
    raise ValueError(f"Colunas obrigatórias ausentes na planilha: {missing}")

# Remove linhas inválidas
df = df.dropna(subset=["ID", "COOPERATIVA"])

# =========================================================
# 3) CONECTAR NO POSTGRESQL
# =========================================================
conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    connect_timeout=10
)
cursor = conn.cursor()

# =========================================================
# 4) GARANTIR QUE A TABELA EXISTA
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
# 5) SQL DE UPSERT (INSERE OU ATUALIZA)
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

# =========================================================
# 6) EXECUTAR CARGA
# =========================================================
for _, row in df.iterrows():
    cursor.execute(
        sql,
        (
            int(row["ID"]),
            str(row["COOPERATIVA"]),
            int(row["META"]) if not pd.isna(row["META"]) else 0,
            row["DATA"] if not pd.isna(row["DATA"]) else None,
            row["STATUS"] if not pd.isna(row["STATUS"]) else None
        )
    )

# =========================================================
# 7) FINALIZAR
# =========================================================
conn.commit()
cursor.close()
conn.close()

print("✅ Sincronização concluída com sucesso (cache ignorado)")
