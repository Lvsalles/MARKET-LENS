import streamlit as st
import re

st.title("DB Debug")

db_url = st.secrets.get("DATABASE_URL", "")
if not db_url:
    st.error("DATABASE_URL is missing from secrets.")
    st.stop()

# Mask password safely
masked = re.sub(r":([^@]+)@", ":*****@", db_url)
st.write("DATABASE_URL loaded:", masked)

# Extract username (between // and :)
m = re.search(r"//([^:]+):", db_url)
st.write("Username loaded:", m.group(1) if m else "NOT FOUND")

st.info("If the username is not EXACTLY what Supabase shows in the Connect popup, it will fail.")
