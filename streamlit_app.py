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
    show_diagnostics = st.toggle("Mostrar Diagnostics", value=True)
    st.caption("Market Lens • Analytics Engine")

# ======================
# DB CONNECTION
# ======================
try:
    engine = get_engine()
    st.success("Banco conectado com sucesso ✅")
except Exception as e:
    st.error("Erro ao conectar no banco")
    st.code(str(e))
    st.stop()

# ======================
# LOAD + CLASSIFY
# ======================
df_raw = read_stg(engine, project_id)
df, detected_cols = classify_rows(df_raw)

# ======================
# OVERVIEW
# ======================
st.header("📈 Overview — Market Snapshot")

if df.empty:
    st.warning("Nenhum dado encontrado.")
    st.stop()

# CARDS
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Registros", len(df))
c2.metric("Listings", int((df["category"] == "Listings").sum()))
c3.metric("Sold", int((df["category"] == "Sold").sum()))
c4.metric("Rentals", int((df["category"] == "Rental").sum()))

# TABLE COUNTS
st.subheader("Distribuição por Categoria")
counts = table_row_counts(df)
st.dataframe(counts, use_container_width=True)

# SOLD METRICS
st.subheader("Métricas de Mercado (Sold)")
df_sold = df[df["category"] == "Sold"].copy()

if df_sold.empty:
    st.warning("Nenhum imóvel vendido encontrado (ainda). Vamos diagnosticar o status abaixo.")
else:
    met = investor_grade_overview(df_sold, detected_cols)
    st.dataframe(met, use_container_width=True)

# ======================
# DIAGNOSTICS
# ======================
if show_diagnostics:
    st.header("🧪 Diagnostics")

    st.subheader("Colunas detectadas automaticamente")
    st.json(detected_cols)

    st.subheader("Preview de colunas-chave (primeiras 25 linhas)")
    cols_preview = []
    for k in ["status_col", "type_col", "price_col", "sqft_col", "adom_col", "beds_col", "baths_col"]:
        c = detected_cols.get(k)
        if c and c in df.columns:
            cols_preview.append(c)

    cols_preview = list(dict.fromkeys(cols_preview))  # unique mantendo ordem
    if cols_preview:
        st.dataframe(df[cols_preview].head(25), use_container_width=True)
    else:
        st.info("Nenhuma coluna-chave foi detectada. O dataset pode ter nomes muito diferentes.")

    # distribuição dos valores de status (TOP 50)
    status_col = detected_cols.get("status_col")
    if status_col and status_col in df.columns:
        st.subheader(f"Top valores em STATUS (coluna: {status_col})")
        vc = df[status_col].astype(str).fillna("").str.upper().value_counts().head(50).reset_index()
        vc.columns = ["status_value", "count"]
        st.dataframe(vc, use_container_width=True)

    # distribuição por category + amostra de linhas que viraram Other
    st.subheader("Amostra de linhas classificadas como Other (para achar o padrão faltando)")
    other = df[df["category"] == "Other"].copy()
    st.write(f"Total Other: {len(other)}")
    if len(other) > 0:
        show_cols = []
        for c in [detected_cols.get("status_col"), detected_cols.get("type_col")]:
            if c and c in other.columns:
                show_cols.append(c)
        show_cols = list(dict.fromkeys(show_cols))
        if show_cols:
            st.dataframe(other[show_cols].head(30), use_container_width=True)
        else:
            st.dataframe(other.head(30), use_container_width=True)

# ======================
# RAW DATA
# ======================
st.header("📄 Dados Brutos (Preview)")
with st.expander("Ver dados brutos"):
    st.dataframe(df.head(1000), use_container_width=True)

st.markdown("---")
st.caption("Market Lens • Data Intelligence for Real Estate")
