import streamlit as st
from sqlalchemy import create_engine

def get_engine():
    if "database" not in st.secrets or "url" not in st.secrets["database"]:
        raise RuntimeError(
            "Missing Streamlit Secrets. Add:\n"
            "[database]\n"
            "url = \"postgresql+psycopg2://...\""
        )

    db_url = st.secrets["database"]["url"]

    return create_engine(
        db_url,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_size=5,
        max_overflow=10,
    )
