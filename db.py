import streamlit as st
import psycopg2
from psycopg2.extras import execute_values


def get_db_conn():
    return psycopg2.connect(st.secrets["DATABASE_URL"], sslmode="require")



def fetch_table_columns(conn, table_name: str, schema: str = "public") -> set[str]:
    """
    Returns the set of column names for a table.
    Used to auto-adapt inserts to the DB schema (very important).
    """
    q = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s;
    """
    with conn.cursor() as cur:
        cur.execute(q, (schema, table_name))
        rows = cur.fetchall()
    return {r[0] for r in rows}


def insert_upload(
    conn,
    filename: str,
    filetype: str,
    dataset_type: str,
    row_count: int,
    col_count: int,
    stored_path: str,
) -> str:
    """
    Inserts into uploads table. Works even if your uploads table has extra columns.
    Assumes 'upload_id' is UUID default gen_random_uuid() OR returns UUID from DB.
    """
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
    return str(upload_id)


def insert_document_text(conn, upload_id: str, content_text: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO document_text (upload_id, content_text)
            VALUES (%s, %s);
            """,
            (upload_id, content_text),
        )
    conn.commit()


def bulk_insert_dicts(conn, table: str, rows: list[dict], columns: list[str]) -> int:
    """
    Fast bulk insert using execute_values.
    - columns must exist in the target table
    - rows must have keys for those columns (missing keys become None)
    """
    if not rows:
        return 0

    values = [[r.get(c) for c in columns] for r in rows]

    with conn.cursor() as cur:
        execute_values(
            cur,
            f'INSERT INTO "{table}" ({", ".join(columns)}) VALUES %s',
            values,
            page_size=1000,
        )
    conn.commit()
    return len(rows)
