# db.py
import streamlit as st
import psycopg2
from psycopg2.extras import execute_values


def get_db_conn():
    """
    Uses DATABASE_URL from Streamlit Cloud Secrets.
    Example:
    postgresql://postgres.<project_ref>:<password>@aws-0-us-west-2.pooler.supabase.com:6543/postgres
    """
    dsn = st.secrets["DATABASE_URL"]
    return psycopg2.connect(dsn, sslmode="require")


def insert_upload(
    conn,
    *,
    report_id,
    filename: str,
    filetype: str,
    dataset_type: str,
    row_count: int,
    col_count: int,
    stored_path: str,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into uploads (report_id, filename, filetype, dataset_type, row_count, col_count, stored_path)
            values (%s, %s, %s, %s, %s, %s, %s)
            returning upload_id;
            """,
            (str(report_id), filename, filetype, dataset_type, row_count, col_count, stored_path),
        )
        upload_id = cur.fetchone()[0]
    conn.commit()
    return upload_id


def insert_document_text(conn, *, upload_id, text: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into document_text (upload_id, content_text)
            values (%s, %s);
            """,
            (str(upload_id), text),
        )
    conn.commit()


def bulk_insert_dicts(conn, *, table: str, rows: list[dict], allowed_cols: list[str]):
    """
    Inserts rows using execute_values for speed.
    IMPORTANT: Make sure you do NOT include the 'id' (bigint) column in allowed_cols.
    """
    if not rows:
        return 0

    cols = allowed_cols
    values = [[r.get(c) for c in cols] for r in rows]

    with conn.cursor() as cur:
        execute_values(
            cur,
            f'insert into {table} ({", ".join(cols)}) values %s',
            values,
            page_size=1000,
        )
    conn.commit()
    return len(rows)
