import streamlit as st
from sqlalchemy import text
from db import get_engine

def main():
    engine = get_engine()
    with engine.connect() as conn:
        v = conn.execute(text("select version()")).fetchone()
        one = conn.execute(text("select 1")).fetchone()
    st.write("VERSION:", v)
    st.write("SELECT 1:", one)

if __name__ == "__main__":
    main()
