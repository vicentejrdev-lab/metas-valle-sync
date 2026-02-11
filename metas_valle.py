import pandas as pd
import psycopg2
from io import StringIO
import requests
import os

# =========================================================
# CONFIGURAÇÕES (via GitHub Secrets / Variáveis de Ambiente)
# =========================================================
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Link público do Google Sheets (CSV)
SHEET_URL = "https://docs.google.com/spreadsheets/d/1oS7VTEOmhaq1hZnns9unXS8qBNJq8yves0dtdZtJUlk/export?format=csv"

# =========================================================
# 1) LER A PLANILHA
# =========================================================
response = requests.get(SHEET_URL, timeout=30)
response.raise_for_status()

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# 🔥 MUDANÇA PRINCIPAL AQUI
# Antes:
# df = pd.read_csv(StringIO(response.text))
#
# Problema: response.text tenta adivinhar encoding
# e quebra acentos (Ã, Ã, Ã...)
#
# Agora: lemos os BYTES reais do arquivo e decodificamos corretamente UTF-8
# utf-8-sig remove o BOM invisível do Google Sheets
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
csv_text = response.content.decode("utf-8-sig")
df = pd.read_csv(StringIO(csv_text), dtype=str)
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

# =========================================================
# 2) NORMALIZAR NOMES DAS COLUNAS
# =========================================================
df.columns = (
    df.columns
      .str.strip()
      .str.upper()
)

# Validação mínima
required_cols = {"ID", "COOPERATIVA", "META", "DATA", "STATUS"}
missing = required_cols - set(df.columns)
if missing:
    raise ValueError(f"Colunas obrigatórias ausentes na planilha: {missing}")

# Remove linhas inválidas
df = df.dropna(subset=["ID", "COOPERATIVA"])

# =========================================================
# 3) CONECTAR NO POSTGRESQL
# =========================================================
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# 🔥 SEGUNDA CORREÇÃO
# Forçamos a conexão usar UTF8 explicitamente
# evita containers linux interpretarem errado
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    connect_timeout=10,
    options='-c client_encoding=UTF8'
)
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

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

    # tratamento de tipos
    id_val = int(row["ID"])
    coop_val = str(row["COOPERATIVA"]).strip()

    meta_val = 0
    if not pd.isna(row["META"]) and row["META"] != "":
        meta_val = int(float(row["META"]))

    data_val = None
    if not pd.isna(row["DATA"]) and row["DATA"] != "":
        data_val = row["DATA"]

    status_val = None
    if not pd.isna(row["STATUS"]) and row["STATUS"] != "":
        status_val = str(row["STATUS"]).strip()

    cursor.execute(
        sql,
        (id_val, coop_val, meta_val, data_val, status_val)
    )

# =========================================================
# 7) FINALIZAR
# =========================================================
conn.commit()
cursor.close()
conn.close()

print("✅ Sincronização concluída com sucesso (acentos preservados)")
