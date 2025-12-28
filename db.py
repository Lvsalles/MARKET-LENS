# db.py
import os
import streamlit as st
from sqlalchemy import create_engine

def get_database_url():
    # 1️⃣ Streamlit Cloud
    if hasattr(st, "secrets") and "SUPABASE_DB_URL" in st.secrets:
        return st.secrets["SUPABASE_DB_URL"]

    # 2️⃣ Ambiente local / docker
    if "SUPABASE_DB_URL" in os.environ:
        return os.environ["SUPABASE_DB_URL"]

    # 3️⃣ Erro explícito
    raise RuntimeError(
        "SUPABASE_DB_URL não encontrada.\n"
        "Defina em Streamlit Secrets ou variável de ambiente."
    )

DATABASE_URL = get_database_url()

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10
)
print("DATABASE_URL =", DATABASE_URL)
