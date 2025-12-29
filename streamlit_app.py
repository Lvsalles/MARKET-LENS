import streamlit as st
from sqlalchemy import text
from db import get_engine
from ai.gemini_ai import analyze_market

st.set_page_config(page_title="Market Lens", layout="wide")

st.title("📊 Market Lens — Inteligência Imobiliária")

# ------------------------
# Conexão com banco
# ------------------------
try:
    engine = get_engine()
    st.success("Banco conectado com sucesso ✅")
except Exception as e:
    st.error("Erro ao conectar no banco")
    st.code(str(e))
    st.stop()

# ------------------------
# Upload / Seleção
# ------------------------
st.sidebar.header("Configurações")

category = st.sidebar.selectbox(
    "Categoria",
    ["Listings", "Pendings", "Sold", "Land", "Rental"]
)

project_id = st.sidebar.text_input("Project ID", "default_project")

# ------------------------
# Carregar dados
# ------------------------
@st.cache_data
def load_data():
    query = f"""
        SELECT *
        FROM normalized_properties
        WHERE category = '{category}'
    """
    return st.read_sql(query, engine)

df = load_data()

if df.empty:
    st.warning("Nenhum dado encontrado para esta categoria.")
    st.stop()

st.success(f"{len(df)} registros carregados")

# ------------------------
# VISÃO GERAL
# ------------------------
st.subheader("📊 Visão Geral")

col1, col2, col3 = st.columns(3)
col1.metric("Registros", len(df))
col2.metric("Preço médio", f"${df['price'].mean():,.0f}")
col3.metric("Preço / sqft", f"${(df['price'] / df['sqft']).mean():,.0f}")

# ------------------------
# IA – ANÁLISE INTELIGENTE
# ------------------------
st.divider()
st.header("🧠 Análise Inteligente (IA)")

if st.button("Gerar análise com IA"):
    with st.spinner("Analisando dados..."):
        insight = analyze_market(df)
        st.markdown("### 📈 Insights do Modelo")
        st.markdown(insight)
