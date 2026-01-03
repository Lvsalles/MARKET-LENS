import streamlit as st
import pandas as pd
from db import get_engine, smoke_test
from etl import insert_staging

st.set_page_config(page_title="Market Lens", layout="wide")
st.title("📊 Market Lens — Fase 1 (Foundation)")

# --------------------
# Conexão
# --------------------
try:
    engine = get_engine()
    smoke_test(engine)
    st.success("Banco conectado com sucesso ✅")
except Exception as e:
    st.error("Erro de conexão")
    st.code(str(e))
    st.stop()

# --------------------
# Upload
# --------------------
st.header("📤 Upload de arquivos (até 12)")

project_id = st.text_input("Project ID", value="default_project")

dataset_type = st.selectbox(
    "Tipo de dados",
    ["properties", "land", "rental"]
)

files = st.file_uploader(
    "Upload XLSX",
    type=["xlsx"],
    accept_multiple_files=True
)

if files and st.button("Importar"):
    total = 0
    for f in files:
        df = pd.read_excel(f)
        rows = insert_staging(engine, df, project_id, dataset_type)
        total += rows

    st.success(f"{total} linhas processadas (sem duplicar)")

# --------------------
# Diagnostics
# --------------------
st.header("🧪 Diagnostics")

with engine.connect() as conn:
    res = conn.execute(
        """
        select dataset_type, status, count(*)
        from stg_raw
        where project_id = :pid
        group by dataset_type, status
        order by dataset_type, status
        """,
        {"pid": project_id}
    ).fetchall()

st.dataframe(res, use_container_width=True)
