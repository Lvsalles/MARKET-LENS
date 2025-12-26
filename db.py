# db.py
import psycopg2
import hashlib
import os
from psycopg2.extras import execute_values
import streamlit as st


def get_db_conn():
    return psycopg2.connect(
        dsn=st.secrets["DATABASE_URL"],
        sslmode="require"
    )


# ----------------------------
# HASH UTIL
# ----------------------------
def compute_file_hash(uploaded_file) -> str:
    data = uploaded_file.getvalue()
    return hashlib.sha256(data).hexdigest()


# ----------------------------
# CHECK IF FILE ALREADY EXISTS
# ----------------------------
def upload_exists(conn, file_hash: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM uploads WHERE file_hash = %s LIMIT 1;",
            (file_hash,)
        )
        return cur.fetchone() is not None


# ----------------------------
# INSERT UPLOAD METADATA
# ----------------------------
def insert_upload(conn, filename, filetype, dataset_type, row_count, col_count, stored_path, file_hash):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO uploads
            (filename, filetype, dataset_type, row_count, col_count, stored_path, file_hash)
            VA
