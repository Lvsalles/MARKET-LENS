import streamlit as st
from sqlalchemy import create_engine

def get_database_url() -> str:
    """
    Retorna a URL do banco vinda do Streamlit Secrets.
    Requer:
      st.secrets["database"]["url"]
    """
    if "database" not in st.secrets or "url" not in st.secrets["database"]:
        raise RuntimeError(
            "Missing database secrets. Configure Streamlit Secrets with:\n"
            "[database]\n"
            "url = \"postgresql+psycopg2://...\""
        )
    return st.secrets["database"]["url"]

def get_engine():
    """
    Cria e retorna um SQLAlchemy engine com configurações seguras para Streamlit Cloud.
    """
    db_url = get_database_url()

    engine = create_engine(
        db_url,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_size=5,
        max_overflow=10
    )
    return engine
