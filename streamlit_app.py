import os
import sys
import streamlit as st
import pandas as pd
from sqlalchemy import text

import streamlit as st
from db import get_engine

st.write("Iniciando app...")

engine = get_engine()

st.success("Banco conectado com sucesso!")

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from db import get_engine  # noqa
from etl import ingest_excel  # noqa
from metrics import compute_group_metrics, rollup_overall_from_zip_facts  # noqa

st.set_page_config(page_title="Market Lens", layout="wide")

engine = get_engine()

# -----------------------------
# Helpers DB
# -----------------------------
def ensure_project(owner_id: str, name: str) -> str:
    with engine.begin() as conn:
        existing = conn.execute(
            text("select id from projects where owner_id=:o and name=:n limit 1"),
            dict(o=owner_id, n=name),
        ).fetchone()

        if existing:
            return str(existing[0])

        pid = conn.execute(
            text("""
                insert into projects(owner_id, name)
                values (:o, :n)
                returning id
            """),
            dict(o=owner_id, n=name),
        ).fetchone()[0]
        return str(pid)

def fetch_norm(project_id: str, category: str) -> pd.DataFrame:
    q = text("""
        select
          zipcode, month_key, category,
          price, sqft, ppsqft, adom,
          beds, baths, garage,
          address, sold_price, sold_date,
          list_agent, sell_agent
        from normalized_properties
        where project_id=:p and category=:c
          and zipcode is not null
          and month_key is not null
    """)
    return pd.read_sql(q, engine, params=dict(p=project_id, c=category))

# -----------------------------
# UI
# -----------------------------
st.title("Market Lens")

with st.sidebar:
    st.header("Projeto")
    owner_id = st.text_input("Owner ID (temporário)", value="demo-owner")  # depois você troca por Supabase Auth
    project_name = st.text_input("Project name", value="North Port 2025")
    project_id = ensure_project(owner_id, project_name)
    st.caption(f"Project ID: {project_id}")

    st.divider()

    st.header("Upload")
    category = st.selectbox("Categoria do arquivo", ["Listings", "Pendings", "Sold", "Land", "Rental"], index=2)
    uploaded = st.file_uploader("Upload Excel (.xlsx)", type=["xlsx"])
    if uploaded is not None:
        if st.button("Ingerir para o banco", type="primary"):
            dataset_id, rows = ingest_excel(
                engine=engine,
                project_id=project_id,
                file_bytes=uploaded.getvalue(),
                filename=uploaded.name,
                category=category,
            )
            st.success(f"Ingestão concluída ✅ dataset_id={dataset_id} rows={rows}")

st.divider()

tab1, tab2, tab3 = st.tabs(["Overview (Overall)", "Tabela Comparativa por ZIP", "Vendas (endereços)"])

# -----------------------------
# 1) Overview Overall
# -----------------------------
with tab1:
    st.subheader("Overall")
    cat = st.selectbox("Categoria para análise", ["Listings", "Pendings", "Sold", "Land", "Rental"], index=2, key="cat_overall")

    df = fetch_norm(project_id, cat)
    if df.empty:
        st.info("Sem dados ainda para essa categoria.")
    else:
        # Métricas por ZIP (para depois fazer overall ponderado pelo count do ZIP)
        zip_rows = []
        for z, g in df.groupby("zipcode"):
            m = compute_group_metrics(g)
            zip_rows.append({"zipcode": z, **m})
        zip_facts = pd.DataFrame(zip_rows).sort_values("record_count", ascending=False)

        overall = rollup_overall_from_zip_facts(zip_facts)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Registros", overall.get("record_count"))
        c2.metric("Avg Price (pond.)", f"${overall.get('avg_price', 0):,.0f}" if overall.get("avg_price") else "—")
        c3.metric("Avg SqFt (pond.)", f"{overall.get('avg_sqft', 0):,.0f}" if overall.get("avg_sqft") else "—")
        c4.metric("Avg ADOM (pond.)", f"{overall.get('avg_adom', 0):,.1f}" if overall.get("avg_adom") else "—")

        c5, c6, c7 = st.columns(3)
        c5.metric("Avg $/SqFt (pond.)", f"${overall.get('avg_ppsqft', 0):,.0f}" if overall.get("avg_ppsqft") else "—")
        c6.metric("Avg Beds (arit.)", f"{overall.get('avg_beds', 0):,.2f}" if overall.get("avg_beds") else "—")
        c7.metric("Avg Baths (arit.)", f"{overall.get('avg_baths', 0):,.2f}" if overall.get("avg_baths") else "—")

        st.caption("Observação: o Overall é calculado a partir das médias por ZIP ponderadas pelo volume do ZIP (proporcionalidade).")
        st.dataframe(zip_facts, use_container_width=True)

# -----------------------------
# 2) ZIP Compare
# -----------------------------
with tab2:
    st.subheader("Tabela comparativa (ZIP)")
    cat = st.selectbox("Categoria", ["Listings", "Pendings", "Sold", "Land", "Rental"], index=2, key="cat_zip")
    df = fetch_norm(project_id, cat)

    if df.empty:
        st.info("Sem dados ainda para essa categoria.")
    else:
        zip_rows = []
        for z, g in df.groupby("zipcode"):
            m = compute_group_metrics(g)
            zip_rows.append({"zipcode": z, **m})
        zip_facts = pd.DataFrame(zip_rows)

        # Formata para leitura
        zip_facts["avg_price"] = zip_facts["avg_price"].round(0)
        zip_facts["avg_sqft"] = zip_facts["avg_sqft"].round(0)
        zip_facts["avg_ppsqft"] = zip_facts["avg_ppsqft"].round(0)
        zip_facts["avg_adom"] = zip_facts["avg_adom"].round(1)
        zip_facts["avg_beds"] = zip_facts["avg_beds"].round(2)
        zip_facts["avg_baths"] = zip_facts["avg_baths"].round(2)
        zip_facts["avg_garage"] = zip_facts["avg_garage"].round(2)

        zip_facts = zip_facts.sort_values(["record_count", "avg_price"], ascending=[False, False])
        st.dataframe(zip_facts, use_container_width=True)

# -----------------------------
# 3) Sold addresses (endereços)
# -----------------------------
with tab3:
    st.subheader("Sold — Endereços vendidos (com métricas)")
    df = fetch_norm(project_id, "Sold")
    if df.empty:
        st.info("Sem dados de Sold ainda.")
    else:
        # Lista simples para agora (depois fazemos filtros e ranking)
        cols = ["sold_date", "zipcode", "address", "sold_price", "sqft", "beds", "baths", "adom", "sell_agent", "list_agent"]
        view = df[cols].copy()
        view["sold_date"] = pd.to_datetime(view["sold_date"], errors="coerce")
        view = view.sort_values("sold_date", ascending=False)
        st.dataframe(view, use_container_width=True)
