import streamlit as st
import pandas as pd
from db import get_engine
from etl import normalize_columns, insert_into_staging

st.set_page_config(page_title="Market Lens", layout="wide")

st.title("📊 Market Lens — Upload de Dados")

# -----------------------------
# Upload múltiplo de arquivos
# -----------------------------
uploaded_files = st.file_uploader(
    "Selecione um ou mais arquivos Excel",
    type=["xlsx"],
    accept_multiple_files=True
)

category = st.selectbox(
    "Tipo de dados",
    ["Listings", "Pendings", "Sold", "Land", "Rental"]
)

project_id = st.text_input("Project ID", value="default_project")

if uploaded_files:
    for uploaded_file in uploaded_files:
        st.subheader(f"📄 Processando: {uploaded_file.name}")

        try:
            df = pd.read_excel(uploaded_file)
            st.success(f"{len(df)} linhas carregadas")

            df = normalize_columns(df)
            insert_into_staging(df, project_id, category)

            st.success(f"Arquivo {uploaded_file.name} importado com sucesso!")

        except Exception as e:
            st.error(f"Erro ao importar {uploaded_file.name}")
            st.code(str(e))
