import os
import psycopg2


def get_db_conn():
    """
    Creates and returns a PostgreSQL connection using DATABASE_URL
    defined in Streamlit Secrets.
    """
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        raise RuntimeError("DATABASE_URL not found in environment variables")

    try:
        conn = psycopg2.connect(
            dsn=database_url,
            sslmode="require"
        )
        return conn
    except Exception as e:
        raise RuntimeError(f"Database connection failed: {e}")
