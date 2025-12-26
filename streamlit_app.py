import streamlit as st
from urllib.parse import quote_plus

st.title("Password Encoder (Temporary Tool)")

pw = st.text_input("Paste your DB password here (it will NOT be stored)", type="password")

if pw:
    encoded = quote_plus(pw)
    st.subheader("Encoded password (copy this):")
    st.code(encoded)
    st.info("Now paste this encoded password into DATABASE_URL in Streamlit Secrets.")
