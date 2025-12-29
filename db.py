import streamlit as st
from sqlalchemy import create_engine

def get_engine():
    # Preferência 1: Streamlit Secrets
    if "database" in st.secrets and "url" in st.secrets["database"]:
        url = st.secrets["database"]["url"]
        return create_engine(url, pool_pre_ping=True)

    # Preferência 2: env var (caso você use)
    import os
    url = os.getenv("SUPABASE_DB_URL")
    if url:
        return create_engine(url, pool_pre_ping=True)

    raise RuntimeError(
        "Missing Streamlit Secrets.\n"
        "Configure no Streamlit Cloud > Secrets:\n"
        "[database]\n"
        'url = "postgresql://..."\n'
    )
