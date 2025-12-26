import streamlit as st
import psycopg2

st.title("Market Lens – Connection Test")

try:
    conn = psycopg2.connect(st.secrets["DATABASE_URL"])
    cur = conn.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")
    tables = cur.fetchall()
    st.success("Connected successfully!")
    st.write("Tables in database:")
    for t in tables:
        st.write("-", t[0])
    cur.close()
    conn.close()
except Exception as e:
    st.error(f"Connection error: {e}")
