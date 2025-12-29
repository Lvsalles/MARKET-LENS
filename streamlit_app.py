import streamlit as st
import pandas as pd
from db import get_engine
from etl import normalize_columns, insert_into_staging

st.set_page_config(page_title="Market Lens", layout="wide")

st.title("📊 Market Lens — Data Ingestion")

# -----------------------------
# Upload de arquivo
# -----------------------------
uploaded_file = st.file_uploader("Upload MLS file (.xlsx)", type=["xlsx"])

category = st.selectbox(
    "Tipo de dados",
    ["Listings", "Pendings", "Sold", "Land", "Rental"]
)

project_id = st.text_input("Project ID", value="default_project")

if uploaded_file:
    df = pd.read_excel(uploaded_file)
    st.success(f"{len(df)} linhas carregadas")

    df = normalize_columns(df)
    st.dataframe(df.head(10))

    if st.button("📥 Importar para o banco"):
        try:
            insert_into_staging(df, project_id, category)
            st.success("Dados inseridos com sucesso!")
        except Exception as e:
            st.error("Erro ao salvar no banco")
            st.code(str(e))
