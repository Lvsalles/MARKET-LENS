import sys
from pathlib import Path
import traceback
import streamlit as st

st.set_page_config(page_title="Market Lens — SAFE MODE")

st.title("🧪 Market Lens — SAFE MODE")

st.write("Iniciando app...")

try:
    ROOT_DIR = Path(__file__).resolve().parent
    if str(ROOT_DIR) not in sys.path:
        sys.path.insert(0, str(ROOT_DIR))

    st.write("✅ Path configurado")

    st.write("Tentando importar run_etl...")
    from backend.etl import run_etl
    st.write("✅ Import run_etl OK")

    st.write("App carregado com sucesso (SAFE MODE)")

except Exception as e:
    st.error("❌ ERRO AO INICIALIZAR O APP")
    st.code(traceback.format_exc())
