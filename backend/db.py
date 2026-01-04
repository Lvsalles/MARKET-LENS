# backend/db.py
from __future__ import annotations

import os
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def get_engine() -> Engine:
    """
    Creates a SQLAlchemy engine for Supabase/Postgres using env var DATABASE_URL.
    Expected env var:
      DATABASE_URL=postgresql+psycopg2://user:pass@host:port/dbname
    """
    db_url = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not db_url:
        raise RuntimeError("Missing DATABASE_URL (or SUPABASE_DB_URL) environment variable.")

    # If user provided "postgresql://", SQLAlchemy will still work,
    # but psycopg2 driver is safest explicit:
    if db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+psycopg2://", 1)

    return create_engine(db_url, pool_pre_ping=True, future=True)
