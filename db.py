import streamlit as st
import psycopg2
from psycopg2.extras import execute_values

def get_db_conn():
    return psycopg2.connect(st.secrets["DATABASE_URL"], sslmode="require")

def insert_upload(
    conn,
    filename: str,
    filetype: str,
    dataset_type: str,
    row_count: int,
    col_count: int,
    stored_path: str
):
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
            f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s",
            values,
            page_size=1000,
        )
    conn.commit()
    return len(rows)
