import streamlit as st
from sqlalchemy import create_engine
import os

st.set_page_config(page_title="Market Lens – DB Init", layout="wide")
st.title("🟢 DB Init")

DATABASE_URL = st.secrets["DATABASE_URL"]

engine = create_engine(DATABASE_URL)
st.success("Engine criada com sucesso")

if st.button("Continuar"):
    st.write("Nenhuma query executada ainda")
