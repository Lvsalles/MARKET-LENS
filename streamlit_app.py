import os
import sys
import streamlit as st
from sqlalchemy import text

# Garante que o diretório atual (onde estão db.py etc.) esteja no PYTHONPATH
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from db import get_engine  # noqa: E402


st.set_page_config(
    page_title="Market Lens",
    layout="wide"
)

st.title("Market Lens — Backend Check")

st.write("✅ App carregou. Agora vamos testar conexão com o Supabase (Postgres).")

try:
    engine = get_engine()

    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1")).fetchone()

    st.success(f"Conexão OK ✅ Resultado do teste: {result}")

except Exception as e:
    st.error("Falha ao conectar no banco ❌")
    st.exception(e)
