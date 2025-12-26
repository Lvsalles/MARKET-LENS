import streamlit as st
from db import get_db_conn

st.set_page_config(page_title="MARKET LENS — Database Connection Test", layout="wide")

st.title("MARKET LENS — Database Connection Test")

if st.button("Test Database Connection"):
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT NOW();")
        now = cur.fetchone()[0]
        cur.close()
        conn.close()

        st.success(f"✅ Connected successfully! Server time: {now}")

    except Exception as e:
        st.error("❌ Connection failed")
        st.exception(e)
