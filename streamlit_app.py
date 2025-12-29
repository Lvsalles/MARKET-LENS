import streamlit as st
from db import get_engine
from metrics import (
    read_stg,
    table_row_counts,
    investor_grade_overview
)

# =========================
# CONFIG (TEM QUE SER PRIMEIRO)
# =========================
st.set_page_config(page_title="Market Lens", layout="wide")
st.title("📊 Market Lens")

# =========================
# Sidebar
# =========================
st.sidebar.header("Configuração")
project_id = st.sidebar.text_input("Project ID", value="default_project")

st.sidebar.caption("Nesta fase, usamos stg_mls. Próximo passo: fact_* + dimensões (modelo final).")

# =========================
# DB connect
# =========================
try:
    engine = get_engine()
    st.success("Banco conectado com sucesso ✅")
except Exception as e:
    st.error("Erro ao conectar no banco ❌")
    st.code(str(e))
    st.stop()

# =========================
# Tabs
# =========================
tab_overview, tab_diagnostics = st.tabs(["Overview", "Diagnostics"])

# =========================
# OVERVIEW
# =========================
with tab_overview:
    st.subheader("Overview (Investor Grade) — com MÉDIA PONDERADA onde faz sentido")

    st.write("Resumo de linhas por categoria (project_id):")
    st.dataframe(table_row_counts(engine, project_id), use_container_width=True)

    st.divider()

    # Base para o Overview: SOLD (melhor para métricas de mercado)
    df_sold = read_stg(engine, project_id, categories=["Sold"])

    st.markdown("### Cards (P25 / Median / P75 + Weighted Avg)")
    cards = investor_grade_overview(df_sold)

    if cards.empty:
        st.info("Sem dados em SOLD para este project_id (ou ainda não importou SOLD).")
    else:
        st.dataframe(cards, use_container_width=True)

    st.divider()
    st.markdown("### Market Snapshot (12m) — MoM/YoY (média ponderada por mês)")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("**Sold Price (weighted avg)**")
        snap_price = monthly_snapshot_weighted(df_sold, "sold_price", "sqft")
        st.dataframe(snap_price.tail(24), use_container_width=True)

    with col2:
        st.markdown("**$/Sqft (weighted avg)**")
        snap_ppsqft = monthly_snapshot_weighted(df_sold, "ppsqft", "sqft")
        st.dataframe(snap_ppsqft.tail(24), use_container_width=True)

    with col3:
        st.markdown("**ADOM (weighted avg)**")
        snap_adom = monthly_snapshot_weighted(df_sold, "adom", "sqft")
        st.dataframe(snap_adom.tail(24), use_container_width=True)

    st.divider()
    st.markdown("### Notas (importante)")
    st.write(
        "- **Peso padrão = sqft** (se faltar sqft, o peso vira 1).\n"
        "- Isso evita distorções por propriedades muito grandes/pequenas.\n"
        "- Na próxima fase, vamos implementar os **presets de segmento** (SFR, Condo, Mobile, 2015+, Builder Box)\n"
        "  e mover tudo para `fact_sold`, `fact_listings`, `fact_land`, `fact_rental`."
    )

# =========================
# DIAGNOSTICS
# =========================
with tab_diagnostics:
    st.subheader("Diagnostics (Qualidade dos Dados)")

    st.write("Contagem por categoria:")
    st.dataframe(table_row_counts(engine, project_id), use_container_width=True)

    st.divider()

    category_diag = st.selectbox("Escolha uma categoria para diagnosticar", ["Listings", "Pendings", "Sold", "Land", "Rental"])
    df = read_stg(engine, project_id, categories=[category_diag])

    st.markdown(f"### Preview — {category_diag}")
    st.write(f"Linhas: **{len(df)}**")
    st.dataframe(df.head(25), use_container_width=True)

    st.divider()
    st.markdown("### Missingness (% vazio por coluna)")
    miss = missingness_report(df)
    st.dataframe(miss.head(50), use_container_width=True)

    st.divider()
    st.markdown("### Duplicidade (mls_id + address)")
    dups = duplicates_report(df)
    if dups.empty:
        st.success("Nenhuma duplicidade detectada (ou colunas ausentes).")
    else:
        st.warning("Duplicidades detectadas:")
        st.dataframe(dups, use_container_width=True)

    st.divider()
    st.markdown("### Outliers (regras simples)")
    out = outliers_report(df)
    st.dataframe(out, use_container_width=True)

    st.divider()
    st.markdown("### Próximo passo")
    st.write(
        "Agora que temos ingestão + diagnostics + overview, o próximo passo é criar:\n"
        "1) **ZIP Compare** (medianas/quartis + MoM/YoY por ZIP)\n"
        "2) **Top 25 Realtors** (volume por status + cards com endereços + link Zillow)\n"
        "3) **Street Analyzer** (2+, 4+, 6+, 10+ vendas)\n"
        "4) **Underpricing Detector** (score 0–100 + explicação)"
    )
