import streamlit as st

st.title("Database Connection Test")

db_url = st.secrets.get("DATABASE_URL", "")
st.write("DATABASE_URL loaded:", db_url.replace(db_url.split(":")[2].split("@")[0], "*****") if db_url else "MISSING")
