# db.py
import streamlit as st
import psycopg2
from psycopg2.extras import execute_values


def get_db_conn():
    """
    Uses Streamlit Cloud Secrets:
      DATABASE_URL = "postgresql://user:pass@host:port/dbname"
    """
    dsn = st.secrets["DATABASE_URL"]  # MUST be exactly this key in Secrets
    return psycopg2.connect(dsn, sslmode="require")


def get_table_columns(conn, table_name: str, schema: str = "public") -> list[str]:
    """
    Returns actual column names in the target table.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position;
            """,
            (schema, table_name),
        )
        return [r[0] for r in cur.fetchall()]


def insert_upload(
    conn,
    filename: str,
    filetype: str,
    dataset_type: str,
    row_count: int,
    col_count: int,
    stored_path: str,
):
    """
    uploads table MUST have these columns:
      filename, filetype, dataset_type, row_count, col_count, stored_path (and upload_id default)
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


def bulk_insert_dicts(conn, table: str, rows: list[dict], schema: str = "public") -> int:
    """
    Inserts only columns that exist in the database table.
    Prevents errors like:
      - column "baths" does not exist
      - column "zip" does not exist

    Also avoids inserting identity columns like "id" (unless you explicitly include it and it exists).
    """
    if not rows:
        return 0

    db_cols = set(get_table_columns(conn, table, schema=schema))

    # Keep only keys that exist in the DB table
    cols = [c for c in rows[0].keys() if c in db_cols]

    if not cols:
        return 0

    values = [[r.get(c) for c in cols] for r in rows]

    with conn.cursor() as cur:
        execute_values(
            cur,
            f'INSERT INTO "{schema}"."{table}" ({", ".join([f\'"{c}"\' for c in cols])}) VALUES %s',
            values,
            page_size=1000,
        )

    conn.commit()
    return len(rows)
