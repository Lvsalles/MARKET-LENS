import sys
from pathlib import Path

# -------------------------------------------------
# Garantir que a raiz do projeto esteja no PYTHONPATH
# (necessário no Streamlit Cloud)
# -------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import streamlit as st
from datetime import date
import tempfile

# Agora o import funciona de forma confiável
from backend.etl import run_etl


# -------------------------------------------------
# UI
# -------------------------------------------------
st.set_page_config(page_title="Market Lens — MLS ETL", layout="wide")

st.title("🏗️ Market Lens — MLS ETL")

uploaded_files = st.file_uploader(
    "Upload de arquivos MLS (.xlsx)",
    type=["xlsx"],
    accept_multiple_files=True,
)

if st.button("▶️ Rodar ETL") and uploaded_files:
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
            st.dataframe(df.head(50), use_container_width=True)

        except Exception as e:
            st.error("Erro ao executar ETL")
            st.exception(e)
