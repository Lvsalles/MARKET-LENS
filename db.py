import psycopg2
import streamlit as st


def get_db_conn():
    if "DATABASE_URL" not in st.secrets:
        raise RuntimeError("DATABASE_URL not found in Streamlit secrets")

    db_url = st.secrets["DATABASE_URL"]

    try:
        conn = psycopg2.connect(
            dbname=db_url.split("/")[-1],
            user=db_url.split("//")[1].split(":")[0],
            password=db_url.split(":")[2].split("@")[0],
            host=db_url.split("@")[1].split(":")[0],
            port=db_url.split(":")[-1],
            sslmode="require",
        )
        return conn

    except Exception as e:
        raise RuntimeError(f"Database connection failed: {e}")
