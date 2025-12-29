import streamlit as st

# 1. Configuração de página (Sempre o primeiro comando)
st.set_page_config(
    page_title="Market Lens",
    layout="wide"
)

# 2. Imports após o config
import os
from sqlalchemy import text
from db import get_engine

# 3. Título da App
st.title("🔍 Market Lens")

# 4. Lógica de Conexão
try:
    engine = get_engine()
    
    # Usando o context manager para garantir que a conexão feche após o uso
    with engine.connect() as conn:
        # SELECT 1 é o teste padrão para ver se o banco responde
        result = conn.execute(text("SELECT 1"))
        # No SQLAlchemy 2.0, é boa prática fechar a transação se necessário
        
    st.success("✅ Conexão com o banco estabelecida com sucesso!")
    
except Exception as e:
    st.error("❌ Erro ao conectar ao banco")
    # Mostra o erro detalhado para diagnóstico
    st.exception(e)
