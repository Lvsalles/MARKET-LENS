import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Market Lens – DB Probe", layout="wide")
st.title("🟢 DB Probe")

# 1️⃣ Lê a string do banco direto dos Secrets
DATABASE_URL = st.secrets["DATABASE_URL"]

# 2️⃣ Cria a engine
engine = create_engine(DATABASE_URL)
st.success("Engine criada com sucesso")

# 3️⃣ Botão para testar query mínima
if st.button("Executar SELECT 1"):
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).fetchone()
        st.success("Query executada com sucesso")
        st.write(result)
    except Exception as e:
        st.error("Erro ao executar query")
        st.exception(e)
