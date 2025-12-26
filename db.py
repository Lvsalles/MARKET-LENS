import psycopg2
from psycopg2.extras import execute_values
import uuid
from datetime import datetime
import streamlit as st


# =========================
# Database connection
# =========================
def get_db_conn():
    return psycopg2.connect(
        st.secrets["DATABASE_URL"],
        sslmode="require"
    )


# =========================
# Upload metadata
# =========================
def insert_upload(
    conn,
    filename: str,
    filetype: str,
    dataset_type: str,
    row_count: int,
    col_count: int,
    stored_path: str
) -> str:
    upload_id = str(uuid.uuid4())

    sql = """
        INSERT INTO uploads (
            upload_id,
            filename,
            filetype,
            dataset_type,
            row_count,
            col_count,
            stored_path,
            created_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """

    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                upload_id,
                filename,
                filetype,
                dataset_type,
                row_count,
                col_count,
                stored_path,
                datetime.utcnow()
            )
        )

    conn.commit()
    return upload_id


# =========================
# Document text storage
# =========================
def insert_document_text(conn, upload_id: str, content_text: str):
    sql = """
        INSERT INTO document_text (
            upload_id,
            content_text,
            created_at
        )
        VALUES (%s,%s,%s)
    """

    with conn.cursor() as cur:
        cur.execute(
            sql,
            (upload_id, content_text, datetime.utcnow())
        )

    conn.commit()


# =========================
# Bulk insert (CORE FIX)
# =========================
def bulk_insert_dicts(
    conn,
    table_name: str,
    rows: list[dict],
    allowed_columns: list[str]
) -> int:

    if not rows:
        return 0

    records = [
        tuple(row.get(col) for col in allowed_columns)
        for row in rows
    ]

    columns_sql = ", ".join(allowed_columns)
    values_sql = ", ".join(["%s"] * len(allowed_columns))

    sql = f"""
        INSERT INTO {table_name} ({columns_sql})
        VALUES %s
    """

    with conn.cursor() as cur:
        execute_values(
            cur,
            sql,
            records,
            template=f"({values_sql})"
        )

    conn.commit()
    return len(records)
