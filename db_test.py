import streamlit as st
import psycopg2

st.title("Database Connection Test")

try:
    conn = psycopg2.connect(
        host="aws-0-us-west-2.pooler.supabase.com",
        port=6543,
        dbname="postgres",
        user="postgres.kzkxqsivqymdtmtyzmqh",
        password="G@bys2010",
        sslmode="require"
    )

    cur = conn.cursor()
    cur.execute("SELECT now();")
    now = cur.fetchone()[0]

    st.success(f"✅ Connected to database! Server time: {now}")

    cur.close()
    conn.close()

except Exception as e:
    st.error("❌ Failed to connect to database")
    st.exception(e)
