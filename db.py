import streamlit as st
from sqlalchemy import create_engine

def get_engine():
    """
    Retorna uma conexão SQLAlchemy usando credenciais do Streamlit Secrets
    """
    if "database" not in st.secrets or "url" not in st.secrets["database"]:
        raise RuntimeError(
            "Configuração ausente: adicione [database] url no secrets.toml"
        )

    db_url = st.secrets["database"]["url"]

    engine = create_engine(
        db_url,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10
    )

    return engine
