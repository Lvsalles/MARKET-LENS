# streamlit_app.py
import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text

# --------------------------------------------------
# CONFIG
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
# SCHEMAS
# --------------------------------------------------
@st.cache_data
def get_schemas():
    q = """
        SELECT schema_name
        FROM information_schema.schemata
        ORDER BY schema_name;
    """
    with engine.connect() as conn:
        return pd.read_sql(text(q), conn)

schemas_df = get_schemas()

st.subheader("Schemas disponíveis")
st.dataframe(schemas_df, use_container_width=True)

schema = st.selectbox(
    "Selecione o schema para explorar",
    schemas_df["schema_nam_]()_
