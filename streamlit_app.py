import streamlit as st
import psycopg2

st.title("Database Connection Test")

try:
    conn = psycopg2.connect(st.secrets["DATABASE_URL"], sslmode="require")
    cur = conn.cursor()
    cur.execute("SELECT 1;")
    st.success("✅ Connected!")
    cur.close()
    conn.close()
except Exception as e:
    st.error(f"❌ Connection failed: {e}")
