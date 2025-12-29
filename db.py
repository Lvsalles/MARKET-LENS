import os
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

def get_engine():
    db_url = os.getenv("DATABASE_URL")

    if not db_url:
        raise RuntimeError(
            "DATABASE_URL não configurado. "
            "Defina a variável no Streamlit Secrets."
        )

    try:
        engine = create_engine(
            db_url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
            connect_args={"connect_timeout": 5},
        )
        return engine
    except Exception as e:
        raise RuntimeError(f"Erro ao criar engine: {str(e)}")
