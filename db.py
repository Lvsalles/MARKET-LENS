import streamlit as st
from sqlalchemy import create_engine

def get_engine():
    # Verifica se o segredo existe antes de tentar usar
    if "database" not in st.secrets or "url" not in st.secrets["database"]:
        st.error("❌ Erro: Segredo 'database.url' não encontrado nos Secrets do Streamlit.")
        st.stop()
        
    conn_url = st.secrets["database"]["url"].strip()
    
    return create_engine(
        conn_url,
        pool_pre_ping=True
    )
