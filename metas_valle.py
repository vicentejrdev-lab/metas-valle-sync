import requests
import pandas as pd
from io import StringIO
import psycopg2
from psycopg2.extras import execute_batch

# =========================
# CONFIGURAÇÕES
# =========================

SHEET_URL = "COLE_AQUI_O_LINK_CSV_DO_GOOGLE_SHEETS"

DB_CONFIG = {
    "host": "SEU_HOST",
    "database": "SEU_BANCO",
    "user": "SEU_USUARIO",
    "password": "SUA_SENHA",
    "port": 5432
}

# =========================
# BAIXAR PLANILHA (UTF-8 CORRETO)
# =========================

print("Baixando planilha...")

response = requests.get(SHEET_URL, timeout=60)
response.raise_for_status()

# IMPORTANTE: usar response.text (UTF8 automático)
df = pd.read_csv(StringIO(response.text))

print("Planilha carregada com sucesso.")

# =========================
# LIMPEZA DE DADOS
# =========================

# remove espaços extras
df.columns = df.columns.str.strip()

# garantir que cooperativa é texto
df["cooperativa"] = df["cooperativa"].astype(str).str.strip()

# converter meta
df["meta"] = pd.to_numeric(df["meta"], errors="coerce")

# converter data
df["data"] = pd.to_datetime(df["data"], errors="coerce")

# remover linhas inválidas
df = df.dropna(subset=["cooperativa", "meta", "data"])

print(f"{len(df)} registros válidos para importar.")

# =========================
# CONEXÃO POSTGRESQL
# =========================

print("Conectando ao banco...")

conn = psycopg2.connect(**DB_CONFIG)
conn.autocommit = False
cursor = conn.cursor()

# =========================
# CRIAR TABELA SE NÃO EXISTIR
# =========================

cursor.execute("""
CREATE TABLE IF NOT EXISTS meta_valle (
    id SERIAL PRIMARY KEY,
    cooperativa TEXT,
    meta NUMERIC,
    data DATE,
    status TEXT DEFAULT 'ATIVO'
);
""")

conn.commit()

# =========================
# INSERT / UPDATE
# =========================

print("Inserindo dados...")

sql = """
INSERT INTO meta_valle (cooperativa, meta, data, status)
VALUES (%s, %s, %s, 'ATIVO')
ON CONFLICT (cooperativa, data)
DO UPDATE SET
    meta = EXCLUDED.meta,
    status = 'ATIVO';
"""

# garantir chave única
cursor.execute("""
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_meta_valle_coop_data'
    ) THEN
        ALTER TABLE meta_valle
        ADD CONSTRAINT uq_meta_valle_coop_data UNIQUE (cooperativa, data);
    END IF;
END$$;
""")

conn.commit()

data_to_insert = [
    (row["cooperativa"], row["meta"], row["data"].date())
    for _, row in df.iterrows()
]

execute_batch(cursor, sql, data_to_insert, page_size=500)

conn.commit()

print("Importação concluída com sucesso!")

# =========================
# CORRIGIR TEXTOS ANTIGOS (MOJIBAKE)
# =========================

print("Corrigindo registros antigos com acento quebrado...")

cursor.execute("""
UPDATE meta_valle
SET cooperativa =
    convert_from(
        convert_to(cooperativa, 'LATIN1'),
        'UTF8'
    )
WHERE octet_length(cooperativa) > length(cooperativa);
""")

conn.commit()

print("Acentos corrigidos.")

# =========================
# FINALIZAR
# =========================

cursor.close()
conn.close()

print("Processo finalizado.")
