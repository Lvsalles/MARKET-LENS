import streamlit as st
import psycopg2

st.title("Teste de Conexão com o Banco")

try:
    conn = psycopg2.connect(st.secrets["DATABASE_URL"])
    cur = conn.cursor()
    cur.execute("SELECT current_database(), current_user;")
    result = cur.fetchone()

    st.success("✅ Conectado com sucesso!")
    st.write("Banco:", result[0])
    st.write("Usuário:", result[1])

    cur.close()
    conn.close()

except Exception as e:
    st.error("❌ Erro ao conectar ao banco")
    st.error(str(e))
