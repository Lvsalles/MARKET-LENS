import streamlit as st

# ⚠️ ESTA LINHA TEM QUE SER A PRIMEIRA CHAMADA STREAMLIT
st.set_page_config(
    page_title="Market Lens",
    layout="wide"
)

# ------------------------------------------------
# Imports AFTER set_page_config
# ------------------------------------------------
import os
import sys
from sqlalchemy import text
from db import get_engine

# ------------------------------------------------
# App
# ------------------------------------------------
st.title("Market Lens")

try:
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    st.success("Conexão com o banco estabelecida com sucesso!")
except Exception as e:
    st.error("Erro ao conectar ao banco")
    st.exception(e)
