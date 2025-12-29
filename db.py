import os
import streamlit as st
from sqlalchemy import create_engine

def get_database_url():
    if "SUPABASE_DB_URL" in st.secrets:
        return st.secrets["SUPABASE_DB_URL"]
    raise RuntimeError("SUPABASE_DB_URL not found")

engine = create_engine(
    get_database_url(),
    pool_pre_ping=True,
)
