import streamlit as st
from sqlalchemy import create_engine

def get_engine():
    db_url = st.secrets["database"]["url"]
    engine = create_engine(
        db_url,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10
    )
    return engine
