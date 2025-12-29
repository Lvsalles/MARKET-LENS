import streamlit as st
from db import get_engine
from metrics import (
    read_stg,
    table_row_counts,
    investor_grade_overview
)

st.set_page_config(page_title="Market Lens", layout="wide")
st.title("📊 Market Lens")

# ======================
# CONEXÃO
# ======================
try:
    engine = get_engine()
    st.success("Banco conectado com sucesso ✅")
except Exception as e:
    st.error("Erro ao conectar ao banco")
    st.code(str(e))
    st.stop()

# ======================
# SIDEBAR
# ======================
st.sidebar.header("Configuração")
project_id = st.sidebar.text_input("Project ID", value="default_project")

# ======================
# OVERVIEW
# ======================
st.header("Overview — Market Snapshot")

# Contagem por tipo
st.subheader("Resumo por tipo")
counts = table_row_counts(engine, project_id)
st.dataframe(counts, use_container_width=True)

# SOLD
st.subheader("Métricas – Sold")
df_sold = read_stg(engine, project_id, "Sold")

if df_sold.empty:
    st.warning("Nenhum registro SOLD encontrado.")
else:
    metrics = investor_grade_overview(df_sold)
    st.dataframe(metrics, use_container_width=True)
