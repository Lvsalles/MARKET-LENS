import streamlit as st

# 1. Configuração de página (Obrigatório ser a primeira linha Streamlit)
st.set_page_config(
    page_title="Market Lens",
    layout="wide"
)

# 2. Imports
from sqlalchemy import text
from db import get_engine

st.title("Market Lens")

try:
    # 3. Obtendo o motor de conexão
    engine = get_engine()
    
    # 4. Testando a conexão de forma segura (SQLAlchemy 2.0+)
    with engine.connect() as conn:
        # É vital usar text() em volta da query
        result = conn.execute(text("SELECT 1"))
        # Confirma que o resultado foi lido
        result.fetchone()
        
    st.success("Conexão com o banco estabelecida com sucesso!")

except Exception as e:
    st.error("Erro ao conectar ao banco")
    st.info("Dica: Verifique se o ID do projeto no usuário está correto (postgres.ID-PROJETO)")
    st.exception(e)
