import streamlit as st
import psycopg2

st.title("DB Connection Test")

try:
    conn = psycopg2.connect(st.secrets["DATABASE_URL"], sslmode="require")
    cur = conn.cursor()
    cur.execute("SELECT now();")
    st.success(f"Connected! Server time: {cur.fetchone()[0]}")
except Exception as e:
    st.error(str(e))
