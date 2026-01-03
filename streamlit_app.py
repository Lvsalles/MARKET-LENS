# streamlit_app.py
import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text

# --------------------------------------------------
# PAGE CONFIG
# --------------------------------------------------
st.set_page_config(
    page_title="Market Lens — Data Explorer",
    layout="wide"
)

st.title("📊 Market Lens — Data Explorer")

# --------------------------------------------------
# DATABASE URL
# --------------------------------------------------
DATABASE_URL = st.secrets.get("DATABASE_URL") or os.getenv("DATABASE_URL")

if not DATABASE_URL:
    st.error("❌ DATABASE_URL não configurado.")
    st.stop()

# --------------------------------------------------
# ENGINE
# --------------------------------------------------
@st.cache_resource
def get_engine():
    return create_engine(DATABASE_URL, pool_pre_ping=True)

engine = get_engine()
st.success("✅ Conectado ao banco com sucesso")

# --------------------------------------------------
# LOAD SCHEMAS
# --------------------------------------------------
@st.cache_data
def load_schemas():
    query = """
        SELECT schema_name
        FROM information_schema.schemata
        ORDER BY schema_name;
    """
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)

schemas_df = load_schemas()

st.subheader("Schemas disponíveis")
st.dataframe(schemas_df, use_container_width=True)

schema_list = schemas_df["schema_name"].tolist()

default_schema_index = (
    schema_list.index("public") if "public" in schema_list else 0
)

schema = st.selectbox(
    "Selecione o schema para explorar",
    schema_list,
    index=default_schema_index
)

# --------------------------------------------------
# LOAD TABLES
# --------------------------------------------------
@st.cache_data
def load_tables(schema_name):
    query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = :schema
        ORDER BY table_name;
    """
    with engine.connect() as conn:
        return pd.read_sql(
            text(query),
            conn,
            params={"schema": schema_name}
        )

tables_df = load_tables(schema)

st.subheader(f"Tabelas no schema `{schema}`
