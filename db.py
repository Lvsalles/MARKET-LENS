import os
import streamlit as st
from sqlalchemy import create_engine, text

def get_database_url():
    if "DATABASE_URL" in st.secrets:
        url = st.secrets["DATABASE_URL"]
    else:
        url = os.getenv("DATABASE_URL")

    if not url:
        raise RuntimeError("DATABASE_URL não configurado")

    if "sslmode=" not in url:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}sslmode=require"

    return url

def get_engine():
    return create_engine(
        get_database_url(),
        pool_pre_ping=True,
        pool_recycle=1800
    )

def smoke_test(engine):
    with engine.connect() as conn:
        conn.execute(text("select 1"))
