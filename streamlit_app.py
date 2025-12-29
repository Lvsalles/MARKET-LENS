import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from etl import load_excel_to_db

st.set_page_config(page_title="Market Lens", layout="wide")

st.title("📊 Market Lens — Real Estate Intelligence")

DB_URL = st.secrets["database"]["url"]
engine = create_engine(DB_URL)

# Upload
st.header("📥 Upload de Arquivos")
uploaded_files = st.file_uploader(
    "Selecione arquivos (XLSX)",
    type=["xlsx"],
    accept_multiple_files=True
)

project_id = st.text_input("Project ID", value="default_project")

if uploaded_files:
    for file in uploaded_files:
        with st.spinner(f"Processando {file.name}..."):
            load_excel_to_db(engine, file, project_id)
        st.success(f"{file.name} importado com sucesso!")

st.divider()

# Overview
st.header("📊 Overview — Market Snapshot")

query = """
SELECT
    category,
    COUNT(*) AS total
FROM stg_mls
WHERE project_id = :pid
GROUP BY category
"""

df = pd.read_sql(query, engine, params={"pid": project_id})
st.dataframe(df, use_container_width=True)
