# db.py
import os
import streamlit as st
from sqlalchemy import create_engine


def get_database_url() -> str:
    """
    Returns the database URL from Streamlit Secrets or environment variables.
    This app is designed to run on Streamlit Cloud.
    """

    # 1️⃣ Streamlit Cloud (PRIORIDADE)
    if "SUPABASE_DB_URL" in st.secrets:
        return st.secrets["SUPABASE_DB_URL"]

    # 2️⃣ Ambiente local (opcional)
    if "SUPABASE_DB_URL" in os.environ:
        return os.environ["SUPABASE_DB_URL"]

    # 3️⃣ Falha explícita (melhor do que erro silencioso)
    raise RuntimeError(
        "SUPABASE_DB_URL not found.\n"
        "Define it in .streamlit/secrets.toml or as an environment variable."
    )


# 🔗 Database URL (Pooler do Supabase)
DATABASE_URL = get_database_url()

# ⚙️ SQLAlchemy Engine
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,   # evita conexões mortas
    pool_size=5,
    max_overflow=10,
)

