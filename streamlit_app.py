# streamlit_app.py
import streamlit as st
from db import get_db_conn

st.set_page_config(page_title="MARKET LENS — Database Connection Test", layout="wide")

st.title("MARKET LENS — Database Connection Test")
st.caption("Click the button to test the Supabase Postgres connection using DATABASE_URL from Secrets.")

if st.button("Test Database Connection"):
    try:
        conn = get_db_conn()
        with conn.cursor() as cur:
            cur.execute("select now();")
            now = cur.fetchone()[0]
        conn.close()
        st.success(f"✅ Connected! Server time: {now}")
    except Exception as e:
        st.error("❌ Connection failed")
        st.exception(e)
