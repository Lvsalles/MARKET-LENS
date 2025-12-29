import streamlit as st
from sqlalchemy import text
from db import get_engine

st.set_page_config(page_title="Market Lens", layout="wide")
st.title("📊 Market Lens — Data Explorer")

# --- CONEXÃO SEGURA ---
try:
    engine = get_engine()
    st.success("Banco conectado com sucesso ✅")
except Exception as e:
    st.error("❌ Erro ao conectar no banco")
    st.code(str(e))
    st.stop()

# --- TESTE DE CONEXÃO REAL ---
try:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
except Exception as e:
    st.error("❌ Falha ao executar query no banco")
    st.code(str(e))
    st.stop()

# --- CARREGAR DADOS DE FORMA SEGURA ---
@st.cache_data
def load_data():
    query = "SELECT * FROM stg_mls LIMIT 5000"
    return pd.read_sql(query, engine)

try:
    df = load_data()
except Exception as e:
    st.error("❌ Erro ao carregar dados")
    st.code(str(e))
    st.stop()

# --- VISUALIZAÇÃO ---
st.subheader("📊 Dados carregados")
st.write(f"Total de registros: {len(df)}")
st.dataframe(df.head(50))
