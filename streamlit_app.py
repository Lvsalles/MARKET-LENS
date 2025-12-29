import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text

# ======================================================
# CONFIGURAÇÃO INICIAL
# ======================================================
st.set_page_config(
    page_title="Market Lens",
    layout="wide"
)

st.title("📊 Market Lens — Base Operacional")

# ======================================================
# CONEXÃO COM BANCO
# ======================================================
def get_engine():
    if "database" not in st.secrets:
        raise RuntimeError("Secrets não encontrados. Configure database.url no Streamlit Cloud.")

    db_url = st.secrets["database"]["url"]
    return create_engine(db_url, pool_pre_ping=True)


# ======================================================
# TESTE DE CONEXÃO
# ======================================================
try:
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    st.success("✅ Conexão com banco estabelecida com sucesso.")
except Exception as e:
    st.error("❌ Erro ao conectar com o banco.")
    st.code(str(e))
    st.stop()

# ======================================================
# FUNÇÃO DE LEITURA SEGURA
# ======================================================
def load_data(project_id: str) -> pd.DataFrame:
    try:
        query = text("""
            SELECT *
            FROM stg_mls
            WHERE project_id = :project_id
        """)
        with engine.begin() as conn:
            df = pd.read_sql(query, conn, params={"project_id": project_id})
        return df
    except Exception as e:
        st.error("Erro ao carregar dados.")
        st.code(str(e))
        return pd.DataFrame()

# ======================================================
# UI
# ======================================================
st.subheader("🔎 Seleção do Projeto")

project_id = st.text_input("Project ID", value="default_project")

if st.button("Carregar dados"):
    df = load_data(project_id)

    if df.empty:
        st.warning("Nenhum dado encontrado para este projeto.")
    else:
        st.success(f"{len(df)} registros carregados com sucesso.")

        st.subheader("📋 Prévia dos Dados")
        st.dataframe(df.head(100), use_container_width=True)

        st.subheader("📊 Distribuição por Status")
        if "status" in df.columns:
            st.dataframe(
                df["status"].value_counts().reset_index().rename(
                    columns={"index": "Status", "status": "Quantidade"}
                )
            )
        else:
            st.warning("Coluna 'status' não encontrada.")

        st.subheader("📈 Estatísticas Básicas")
        st.dataframe(df.describe(include="all"))

# ======================================================
# RODAPÉ
# ======================================================
st.markdown("---")
st.caption("Market Lens · Pipeline estável · Pronto para evolução")
