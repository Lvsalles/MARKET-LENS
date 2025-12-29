import streamlit as st
from metrics import read_stg, classify_rows, table_row_counts
from db import get_engine

st.set_page_config(page_title="Market Lens", layout="wide")

st.title("📊 Market Lens — Real Estate Intelligence")

# =====================
# CONEXÃO
# =====================
try:
    engine = get_engine()
    st.success("Banco conectado com sucesso")
except Exception as e:
    st.error("Erro ao conectar no banco")
    st.stop()

# =====================
# INPUT
# =====================
project_id = st.text_input("Project ID", value="default_project")

# =====================
# LOAD DATA
# =====================
df = read_stg(engine, project_id)

if df.empty:
    st.warning("Nenhum dado encontrado para este projeto.")
    st.stop()

df, meta = classify_rows(df)

# =====================
# DASHBOARD
# =====================
st.header("📊 Market Snapshot")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Registros", len(df))
c2.metric("Listings", (df["category"] == "Listings").sum())
c3.metric("Sold", (df["category"] == "Sold").sum())
c4.metric("Rental", (df["category"] == "Rental").sum())

# ---------------------
st.subheader("Distribuição por Categoria")
st.dataframe(table_row_counts(df), use_container_width=True)

# ---------------------
st.subheader("Preview dos Dados")
st.dataframe(df.head(50), use_container_width=True)

# ---------------------
st.caption("Market Lens • Real Estate Intelligence Engine")
