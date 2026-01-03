import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Market Lens – DB Explorer", layout="wide")
st.title("🧠 Market Lens – DB Explorer")

DATABASE_URL = st.secrets["DATABASE_URL"]
engine = create_engine(DATABASE_URL)

st.success("Conectado ao banco com sucesso")

# 1️⃣ Listar schemas
st.subheader("Schemas disponíveis")

schemas_query = """
SELECT schema_name
FROM information_schema.schemata
ORDER BY schema_name;
"""

with engine.connect() as conn:
    schemas = pd.read_sql(text(schemas_query), conn)

st.dataframe(schemas, use_container_width=True)

# 2️⃣ Selecionar schema
schema = st.selectbox(
    "Selecione o schema para explorar",
    schemas["schema_name"].tolist(),
    index=schemas["schema_name"].tolist().index("public") if "public" in schemas["schema_name"].tolist() else 0
)

# 3️⃣ Listar tabelas do schema
st.subheader(f"Tabelas no schema `{schema}`")

tables_query = """
SELECT table_name
FROM information_schema.tables
WHERE table_schema = :schema
ORDER BY table_name;
"""

with engine.connect() as conn:
    tables = pd.read_sql(text(tables_query), conn, params={"schema": schema})

st.dataframe(tables, use_container_width=True)

# 4️⃣ Selecionar tabela
if not tables.empty:
    table = st.se
