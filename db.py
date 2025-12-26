# db.py
import streamlit as st
import psycopg2
from psycopg2.extras import execute_values
from urllib.parse import urlparse, parse_qs


def get_db_conn():
    """
    Cloud-safe connection:
    - Reads DATABASE_URL from Streamlit Secrets
    - Parses correctly (no broken '6543/postgres' port bug)
    - Forces sslmode=require
    """
    dsn = st.secrets["DATABASE_URL"].strip()

    # psycopg2 can accept a DSN string directly, but we also enforce sslmode safely.
    # If the DSN already contains sslmode, we keep it; otherwise we add it.
    if "sslmode=" not in dsn:
        if "?" in dsn:
            dsn = dsn + "&sslmode=require"
        else:
            dsn = dsn + "?sslmode=require"

    # Parse to ensure host/port/dbname are correct (and avoid the bug you're seeing)
    u = urlparse(dsn)
    q = parse_qs(u.query)

    user = u.username
    password = u.password
    host = u.hostname
    port = u.port
    dbname = (u.path or "").lstrip("/") or "postgres"
    sslmode = q.get("sslmode", ["require"])[0]

    if port is None:
        raise RuntimeError("DATABASE_URL has no port. For Supabase Session Pooler, use :6543.")

    return psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=int(port),
        sslmode=sslmode,
    )


def insert_upload(conn, filename: str, filetype: str, dataset_type: str, row_count: int, col_count: int, stored_path: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO uploads (filename, filetype, dataset_type, row_count, col_count, stored_path)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING upload_id;
            """,
            (filename, filetype, dataset_type, row_count, col_count, stored_path),
        )
        upload_id = cur.fetchone()[0]
    conn.commit()
    return upload_id


def insert_document_text(conn, upload_id, text: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO document_text (upload_id, content_text)
            VALUES (%s, %s);
            """,
            (upload_id, text),
        )
    conn.commit()


def bulk_insert_dicts(conn, table: str, rows: list[dict], allowed_cols: list[str]):
    if not rows:
        return 0

    cols = allowed_cols
    values = [[r.get(c) for c in cols] for r in rows]

    with conn.cursor() as cur:
        execute_values(
            cur,
            f'INSERT INTO "{table}" ({", ".join(cols)}) VALUES %s',
            values,
            page_size=1000,
        )

    conn.commit()
    return len(rows)
