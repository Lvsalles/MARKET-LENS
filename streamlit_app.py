import os
import sys
import streamlit as st
from sqlalchemy import text

# Garante que o Python encontre db.py
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from db import get_engine  # <-- agora existe

st.set_page_config(page_title="Market Lens", layout="wide")

st.title("Market Lens – Database Connection Test")

try:
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1")).fetchone()
    st.success(f"Conexão OK: {result}")
except Exception as e:
    st.error("Falha ao conectar no banco ❌")
    st.exception(e)
