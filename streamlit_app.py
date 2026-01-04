import sys
from pathlib import Path
import traceback
import streamlit as st

st.set_page_config(page_title="Market Lens")

st.title("Market Lens — MLS ETL")

# --------------------
# SAFE INIT
# --------------------
try:
    ROOT_DIR = Path(__file__).resolve().parent
    if str(ROOT_DIR) not in sys.path:
        sys.path.insert(0, str(ROOT_DIR))

    from backend.etl import run_etl
    st.success("Backend carregado com sucesso")

except Exception:
    st.error("Erro ao inicializar o backend")
    st.code(traceback.format_exc())
    st.stop()

# --------------------
# UI
# --------------------
uploaded_files = st.file_uploader(
    "Upload de arquivos MLS (.xlsx)",
    type=["xlsx"],
    accept_multiple_files=True
)

if st.button("▶ Executar ETL"):
    if not uploaded_files:
        st.warning("Nenhum arquivo selecionado")
        st.stop()

    st.info("Executando ETL...")

    try:
        result = run_etl(uploaded_files)

        st.success("ETL executado com sucesso")
        st.write(result)

    except Exception:
        st.error("❌ Erro ao executar ETL")
        st.code(traceback.format_exc())
