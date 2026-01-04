# backend/db.py
import os
from sqlalchemy import create_engine

def get_engine():
    """
    Cria e retorna o SQLAlchemy engine usando a variável de ambiente DATABASE_URL
    Compatível com Supabase / Streamlit Cloud
    """

    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        raise RuntimeError("Variável de ambiente DATABASE_URL não definida")

    return create_engine(
        database_url,
        pool_pre_ping=True,
        future=True
    )
