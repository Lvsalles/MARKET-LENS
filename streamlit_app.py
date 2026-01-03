import streamlit as st

st.set_page_config(page_title="Market Lens – Import Test", layout="wide")

st.title("🧪 TESTE DE IMPORTS")

try:
    import pandas as pd
    st.success("pandas OK")
except Exception as e:
    st.error(f"pandas ERRO: {e}")

try:
    import sqlalchemy
    st.success("sqlalchemy OK")
except Exception as e:
    st.error(f"sqlalchemy ERRO: {e}")

try:
    import psycopg2
    st.success("psycopg2 OK")
except Exception as e:
    st.error(f"psycopg2 ERRO: {e}")
