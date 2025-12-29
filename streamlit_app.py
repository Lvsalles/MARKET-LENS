import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import os

# -----------------------------
# CONFIG
# -----------------------------
st.set_page_config(
    page_title="Market Lens",
    layout="wide"
)

st.title("📊 Market Lens – Data Explorer")

# -----------------------------
# DATABASE CONNECTION
# -----------------------------
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    st.error("❌ DATABASE_URL não configurado.")
    st.stop()

engine = create_engine(DATABASE_URL)

# -----------------------------
# LOAD SCHEMA SAFELY
# -----------------------------
@st.cache_data
def get_columns():
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'stg_mls'
            ORDER BY ordinal_position
        """))
        return [r[0] for r in result]

columns = get_columns()

st.success("Conectado ao banco com sucesso ✅")
st.write("### Colunas detectadas:")
st.write(columns)

# -----------------------------
# LOAD DATA (SAFE)
# -----------------------------
@st.cache_data
def load_data():
    query = "SELECT * FROM stg_mls LIMIT 5000"
    return pd.read_sql(query, engine)

df = load_data()

# -----------------------------
# VISUALIZAÇÃO BÁSICA
# -----------------------------
st.subheader("📊 Preview dos Dados")
st.dataframe(df.head(50), use_container_width=True)

# -----------------------------
# CONTADORES BÁSICOS (SE EXISTIREM)
# -----------------------------
st.subheader("📈 Resumo Geral")

cols = df.columns.str.lower()

def safe_count(col):
    return df[col].notna().sum() if col in df.columns else "N/A"

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Registros", len(df))
with col2:
    st.metric("Com Preço", safe_count("list_price"))
with col3:
    st.metric("Com Status", safe_count("status") if "status" in cols else "N/A")

# -----------------------------
# VISUALIZAÇÃO POR STATUS (SE EXISTIR)
# -----------------------------
if "status" in df.columns:
    st.subheader("Distribuição por Status")
    st.bar_chart(df["status"].value_counts())

# -----------------------------
# DEBUG FINAL
# -----------------------------
with st.expander("🔍 Debug – Estrutura Completa"):
    st.write(df.dtypes)
