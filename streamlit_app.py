import streamlit as st

# -------------------------------------------------
# DEBUG GUARANTEE
# -------------------------------------------------
st.write("✅ Streamlit carregou o arquivo streamlit_app.py")

try:
    st.write("🔍 Tentando importar ETL...")
    from backend.etl import run_etl
    st.success("Import do ETL: OK")

except Exception as e:
    st.error("❌ Erro ao importar backend.etl")
    st.exception(e)
    st.stop()

from datetime import date
import tempfile
import pandas as pd

# -------------------------------------------------
# Page config
# -------------------------------------------------
st.set_page_config(
    page_title="Market Lens — ETL",
    layout="wide",
)

st.title("🏗️ Market Lens — MLS ETL (DEBUG MODE)")
st.caption("Diagnóstico de tela branca")

# -------------------------------------------------
# Teste de variável de ambiente
# -------------------------------------------------
st.subheader("🔐 Environment Check")

import os
db_url = os.getenv("DATABASE_URL")

if not db_url:
    st.error("DATABASE_URL NÃO está definida no ambiente ❌")
else:
    st.success("DATABASE_URL encontrada ✅")

# -------------------------------------------------
# Upload
# -------------------------------------------------
uploaded_files = st.file_uploader(
    "Selecione um ou mais arquivos XLSX",
    type=["xlsx"],
    accept_multiple_files=True,
)

if st.button("▶️ Rodar ETL (DEBUG)") and uploaded_files:
    with st.spinner("Processando arquivos..."):
        try:
            temp_paths = []
            for f in uploaded_files:
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
                tmp.write(f.read())
                tmp.close()
                temp_paths.append(tmp.name)

            df = run_etl(
                xlsx_files=temp_paths,
                snapshot_date=date.today(),
                persist=True,
            )

            st.success("ETL executado com sucesso!")
            st.dataframe(df.head(20))

        except Exception as e:
            st.error("❌ Erro durante execução do ETL")
            st.exception(e)
