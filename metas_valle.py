import pandas as pd
import psycopg2
from io import StringIO
import requests
import os
import time
import sys

# =========================================================
# CONFIGURAÇÕES (GitHub Secrets)
# =========================================================
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Validação das variáveis de ambiente
if not all([DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD]):
    print("❌ Variáveis de ambiente do banco não configuradas no GitHub Secrets.")
    sys.exit(1)

# Planilha Google
SHEET_URL = "https://docs.google.com/spreadsheets/d/1oS7VTEOmhaq1hZnns9unXS8qBNJq8yves0dtdZtJUlk/export?format=csv"

# =========================================================
# 1) LER PLANILHA
# =========================================================
print("📥 Baixando planilha...")

response = requests.get(
    f"{SHEET_URL}&_ts={int(time.time())}",
    timeout=30
)
response.raise_for_status()

df = pd.read_csv(StringIO(response.text))

if df.empty:
    print("❌ A planilha veio vazia.")
    sys.exit(1)

# =========================================================
# 2) NORMALIZAR COLUNAS
# =========================================================
df.columns = df.columns.str.strip().str.upper()

required_cols = {"ID", "COOPERATIVA", "META", "DATA", "STATUS"}
missing = required_cols - set(df.columns)

if missing:
    print(f"❌ Colunas ausentes na planilha: {missing}")
    sys.exit(1)

df = df.dropna(subset=["ID", "COOPERATIVA"])

# conversões seguras
df["ID"] = df["ID"].astype(int)
df["META"] = df["META"].fillna(0).astype(int)
df["DATA"] = pd.to_datetime(df["DATA"], errors="coerce").dt.date

# =========================================================
# 3) CONECTAR BANCO
# =========================================================
print("🔌 Conectando ao banco...")

try:
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        connect_timeout=10
    )
except Exception as e:
    print("❌ Falha ao conectar no banco:")
    print(e)
    sys.exit(1)

cursor = conn.cursor()

# =========================================================
# 4) GARANTIR ESTRUTURA
# =========================================================
cursor.execute("""
CREATE TABLE IF NOT EXISTS meta_valle (
    id_cooperativa INT PRIMARY KEY,
    cooperativa VARCHAR(100) NOT NULL,
    meta INT NOT NULL DEFAULT 0,
    data DATE,
    status VARCHAR(100)
);
""")
conn.commit()

# =========================================================
# 5) UPSERT
# =========================================================
sql = """
INSERT INTO meta_valle (id_cooperativa, cooperativa, meta, data, status)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (id_cooperativa)
DO UPDATE SET
    cooperativa = EXCLUDED.cooperativa,
    meta = EXCLUDED.meta,
    data = EXCLUDED.data,
    status = EXCLUDED.status;
"""

# =========================================================
# 6) CARGA
# =========================================================
print("📊 Sincronizando metas...")

registros = 0

for _, row in df.iterrows():
    cursor.execute(
        sql,
        (
            int(row["ID"]),
            str(row["COOPERATIVA"]),
            int(row["META"]),
            row["DATA"],
            None if pd.isna(row["STATUS"]) else str(row["STATUS"])
        )
    )
    registros += 1

# =========================================================
# 7) FINALIZAR
# =========================================================
conn.commit()
cursor.close()
conn.close()

print(f"✅ Metas sincronizadas! Registros processados: {registros}")
