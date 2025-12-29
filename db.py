# db.py
import os
from sqlalchemy import create_engine

def get_engine():
    db_url = os.getenv("SUPABASE_DB_URL")

    if not db_url:
        raise RuntimeError("SUPABASE_DB_URL não definida no ambiente")

    return create_engine(
        db_url,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
    )
