import streamlit as st
from urllib.parse import quote

st.title("Password Encoder")

pw = st.text_input("Paste DB password", type="password")
if pw:
    st.code(quote(pw, safe=""))
