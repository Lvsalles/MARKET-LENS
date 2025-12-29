import streamlit as st
import pandas as pd
from db import get_engine
from etl import normalize_columns, insert_into_staging

st.set_page_config(page_title="Market Lens", layout="wide")

st.title("📊 Market Lens — Upload de Dados")

# =========================
# Upload
# =========================
uploaded_files = st.file_uploader(
    "Selecione um ou mais arquivos Excel",
    type=["xlsx"],
    accept_multiple_files=True
)

category = st.selectbox(
    "Tipo de dados",
    ["Listings", "Pending", "Sold", "Land", "Rental"]
)

project_id = st.text_input("Project ID", value="default_project")

st.divider()

if uploaded_files:
    for uploaded_file in uploaded_files:
        st.subheader(f"📄 Processando: {uploaded_file.name}")

        try:
            df = pd.read_excel(uploaded_file)
            st.success(f"Arquivo lido: {len(df)} linhas")

            # NORMALIZA
            df = normalize_columns(df)
            st.write("Preview dos dados:")
            st.dataframe(df.head(10))

            # INSERÇÃO
            insert_into_staging(df, project_id, category)

            st.success(f"✅ Importado com sucesso: {uploaded_file.name}")

        except Exception as e:
            st.error(f"❌ Erro ao importar {uploaded_file.name}")
            st.code(str(e))

else:
    st.info("Envie um ou mais arquivos Excel para começar.")
