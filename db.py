# db.py
import os
from sqlalchemy import create_engine

def get_engine():
    db_url = os.getenv("SUPABASE_DB_URL")

    if not db_url:
        raise RuntimeError("SUPABASE_DB_URL não definida no ambiente")

    engine = create_engine(
        db_url,
        pool_pre_ping=True,
        pool_size=3,
        max_overflow=2,
        connect_args={"connect_timeout": 5},
    )

    return engine
