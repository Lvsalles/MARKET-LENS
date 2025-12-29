import streamlit as st
import pandas as pd

from db import get_engine
from metrics import (
    read_stg,
    classify_rows,
    table_row_counts,
    investor_grade_overview
)

# =============================
# CONFIG
# =============================
st.set_page_config(
    page_title="Market Lens",
    layout="wide",
    page_icon="📊"
)

st.title("📊 Market Lens — Real Estate Intelligence")

# =============================
# SIDEBAR
# =============================
with st.sidebar:
    st.header("Configurações")
    project_id = st.text_input("Project ID", value="default_project")
    st.markdown("---")
    st.caption("Market Lens • Analytics Engine")

# =============================
# DATABASE
# =============================
try:
    engine = get_engine()
    st.success("Banco conectado com sucesso")
except Exception as e:
    st.error("Erro ao conectar no banco")
    st.code(str(e))
    st.stop()

# =============================
# LOAD DATA
# =============================
df = read_stg(engine, project_id)
df = classify_rows(df)

# =============================
# OVERVIEW SECTION
# =============================
st.header("📊 Overview — Market Snapshot")

if df.empty:
    st.warning("Nenhum dado encontrado para este projeto.")
    st.stop()

# ---- Summary Cards ----
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Registros", len(df))

with col2:
    st.metric("Listings", (df["category"] == "Listings").sum())

with col3:
    st.metric("Sold", (df["category"] == "Sold").sum())

with col4:
    st.metric("Rentals", (df["category"] == "Rental").sum())

# =============================
# TABLE: DISTRIBUIÇÃO POR TIPO
# =============================
st.subheader("Distribuição por Categoria")

counts = (
    df.groupby("category")
    .size()
    .reset_index(name="Total")
    .sort_values("Total", ascending=False)
)

st.dataframe(counts, use_container_width=True)

# =============================
# MÉTRICAS DE MERCADO
# =============================
st.subheader("Métricas de Mercado (Sold)")

df_sold = df[df["category"] == "Sold"]

if df_sold.empty:
    st.warning("Nenhum imóvel vendido encontrado.")
else:
    metrics = investor_grade_overview(df_sold)
    st.dataframe(metrics, use_container_width=True)

# =============================
# TABELA COMPLETA
# =============================
st.subheader("Dados Brutos (Preview)")

with st.expander("Ver dados brutos"):
    st.dataframe(df.head(1000), use_container_width=True)

# =============================
# FOOTER
# =============================
st.markdown("---")
st.caption("Market Lens • Data Intelligence for Real Estate")
