from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import os

def get_engine():
    db_url = os.getenv("SUPABASE_DB_URL")

    if not db_url:
        raise RuntimeError("SUPABASE_DB_URL não definida")

    try:
        engine = create_engine(
            db_url,
            pool_pre_ping=True,
            pool_size=3,
            max_overflow=0,
            connect_args={"connect_timeout": 5}  # 🔥 evita ficar rodando
        )

        # Teste imediato
        with engine.connect() as conn:
            conn.execute("SELECT 1")

        return engine

    except OperationalError as e:
        raise RuntimeError(f"Falha ao conectar no Supabase: {e}")
