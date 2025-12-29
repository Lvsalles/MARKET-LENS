import streamlit as st
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

def get_engine():
    # Se você configurou SUPABASE_DB_URL no Secrets do Streamlit, 
    # vamos extrair os dados ou usar a URL direto.
    # A forma mais garantida para o erro "Tenant not found" é reconstruir a URL:
    
    try:
        # Tenta pegar a string dos secrets
        connection_url = st.secrets["SUPABASE_DB_URL"]
        
        # Criamos o engine com pool_pre_ping para evitar que conexões inativas derrubem o app
        return create_engine(
            connection_url,
            pool_pre_ping=True,
            pool_recycle=300
        )
    except Exception as e:
        st.error("Falha ao carregar SUPABASE_DB_URL dos Secrets.")
        raise e
