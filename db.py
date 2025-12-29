import streamlit as st
from sqlalchemy import create_engine, URL

def get_engine():
    """
    Cria a conexão com o banco de dados Supabase usando os segredos do Streamlit.
    """
    # É mais seguro construir a URL parte por parte para evitar erros de caractere especial
    try:
        # Pega a URL bruta dos secrets (opcional, caso queira manter compatibilidade)
        # Mas a melhor prática para o erro "Tenant not found" é garantir estes campos:
        db_url = URL.create(
            drivername="postgresql+psycopg2",
            username="postgres.kzkxqsivqymdtmtyzmqh",
            password="G@bys2010", # Coloque a senha real aqui ou via st.secrets
            host="aws-0-us-east-1.pooler.supabase.com",
            port=6543,
            database="postgres",
            query={"sslmode": "require"}
        )
        
        # O pool_pre_ping=True ajuda a evitar erros de conexão caída no Streamlit
        return create_engine(db_url, pool_pre_ping=True)
    
    except Exception as e:
        raise Exception(f"Erro ao configurar o motor de banco de dados: {e}")
