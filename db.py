# db.py
import streamlit as st
import psycopg2
from psycopg2.extras import execute_values


def get_db_conn():
    """
    Uses DATABASE_URL from Streamlit Secrets.
    Must be a full DSN like:
    postgresql://postgres.<project_ref>:<PASSWORD>@aws-0-<region>.pooler.supabase.com:6543/postgres
    """
    dsn = st.secrets["DATABASE_URL"]
    return psycopg2.connect(dsn, sslmode="require")


def upload_exists_by_hash(conn, file_hash: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("select 1 from public.uploads where file_hash = %s limit 1;", (file_hash,))
        return cur.fetchone() is not None


def get_upload_id_by_hash(conn, file_hash: str):
    with conn.cursor() as cur:
        cur.execute("select upload_id from public.uploads where file_hash = %s limit 1;", (file_hash,))
        row = cur.fetchone()
        return row[0] if row else None


def insert_upload(
    conn,
    filename: str,
    filetype: str,
    dataset_type: str,
    row_count: int,
    col_count: int,
    stored_path: str,
    file_hash: str,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.uploads (filename, filetype, dataset_type, row_count, col_count, stored_path, file_hash)
            values (%s, %s, %s, %s, %s, %s, %s)
            returning upload_id;
            """,
            (filename, filetype, dataset_type, row_count, col_count, stored_path, file_hash),
        )
        upload_id = cur.fetchone()[0]
    conn.commit()
    return upload_id


def insert_document_text(conn, upload_id, text: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.document_text (upload_id, content_text)
            values (%s, %s);
            """,
            (upload_id, text),
        )
    conn.commit()


def bulk_insert_dicts(conn, table: str, rows: list[dict], allowed_cols: list[str], page_size: int = 2000):
    if not rows:
        return 0

    cols = allowed_cols
    values = [[r.get(c) for c in cols] for r in rows]

    with conn.cursor() as cur:
        execute_values(
            cur,
            f"insert into public.{table} ({', '.join(cols)}) values %s",
            values,
            page_size=page_size,
        )
    conn.commit()
    return len(rows)


def log_run_start(conn, notes: str = ""):
    with conn.cursor() as cur:
        cur.execute(
            "insert into public.ingestion_runs (notes) values (%s) returning run_id;",
            (notes,),
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def log_run_end(conn, run_id, status: str = "success"):
    with conn.cursor() as cur:
        cur.execute(
            "update public.ingestion_runs set ended_at = now(), status = %s where run_id = %s;",
            (status, run_id),
        )
    conn.commit()


def log_event(conn, run_id, upload_id, level: str, message: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.ingestion_events (run_id, upload_id, level, message)
            values (%s, %s, %s, %s);
            """,
            (run_id, upload_id, level, message),
        )
    conn.commit()
