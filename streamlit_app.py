import streamlit as st

# ✅ TEM QUE SER O PRIMEIRO COMANDO STREAMLIT
st.set_page_config(page_title="Market Lens", layout="wide")

import pandas as pd
from sqlalchemy import text

from db import get_engine
from etl import ingest_excel
from metrics import compute_metrics_for_group, rollup_overall_from_zip_table


def fetch_project_data(engine, project_id: str, category: str) -> pd.DataFrame:
    q = text("""
        select
          zipcode, month_key, category,
          price, sqft, ppsqft, adom,
          beds, baths, garage,
          address, sold_price, sold_date,
          list_agent, sell_agent
        from normalized_properties
        where project_id=:p and category=:c
    """)
    return pd.read_sql(q, engine, params={"p": project_id, "c": category})


st.title("Market Lens")

# Conexão (falha rápido se pooler / dns / secrets)
try:
    engine = get_engine()
    st.success("Banco conectado com sucesso ✅")
except Exception as e:
    st.error("Erro ao conectar ao banco ❌")
    st.code(str(e))
    st.stop()

# Sidebar
with st.sidebar:
    st.header("Projeto")
    owner_id = st.text_input("Owner ID", value="demo-owner")
    project_name = st.text_input("Project name", value="Market-Lens")

    st.divider()
    st.header("Upload")
    category = st.selectbox("Categoria", ["Listings", "Pendings", "Sold", "Land", "Rental"], index=2)
    file = st.file_uploader("Excel (.xlsx)", type=["xlsx"])

    if file and st.button("Importar para o banco", type="primary"):
        try:
            result = ingest_excel(
                engine=engine,
                owner_id=owner_id,
                project_name=project_name,
                category=category,
                filename=file.name,
                file_bytes=file.getvalue(),
            )
            st.success(f"Import OK ✅ inserted={result['inserted']} rows={result['rows']}")
            st.session_state["last_project_id"] = result["project_id"]
        except Exception as e:
            st.error("Falha no import ❌")
            st.exception(e)

project_id = st.session_state.get("last_project_id", None)

tabs = st.tabs(["Overview", "ZIP Compare", "Sold Addresses", "Diagnostics"])

with tabs[3]:
    st.subheader("Diagnostics")
    st.write("Se der erro aqui, o problema é URL/host/porta.")
    try:
        with engine.connect() as conn:
            v = conn.execute(text("select version()")).fetchone()
        st.success("Query OK ✅")
        st.write(v)
    except Exception as e:
        st.error("Query falhou ❌")
        st.exception(e)

with tabs[0]:
    st.subheader("Overview (Overall)")
    if not project_id:
        st.info("Importe um arquivo para gerar o project_id e começar as análises.")
    else:
        cat = st.selectbox("Categoria (Overview)", ["Listings", "Pendings", "Sold", "Land", "Rental"], index=2, key="ov_cat")
        df = fetch_project_data(engine, project_id, cat)

        if df.empty:
            st.info("Sem dados para esta categoria.")
        else:
            rows = []
            for z, g in df.groupby("zipcode"):
                m = compute_metrics_for_group(g)
                rows.append({"zipcode": z, **m})
            zip_table = pd.DataFrame(rows).sort_values("record_count", ascending=False)

            overall = rollup_overall_from_zip_table(zip_table)

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Registros", overall.get("record_count"))
            c2.metric("Avg Price (pond.)", f"${overall['avg_price']:,.0f}" if overall.get("avg_price") else "—")
            c3.metric("Avg SqFt (pond.)", f"{overall['avg_sqft']:,.0f}" if overall.get("avg_sqft") else "—")
            c4.metric("Avg ADOM (pond.)", f"{overall['avg_adom']:.1f}" if overall.get("avg_adom") else "—")

            c5, c6, c7 = st.columns(3)
            c5.metric("Avg $/SqFt (pond.)", f"${overall['avg_ppsqft']:,.0f}" if overall.get("avg_ppsqft") else "—")
            c6.metric("Avg Beds (arit.)", f"{overall['avg_beds']:.2f}" if overall.get("avg_beds") else "—")
            c7.metric("Avg Baths (arit.)", f"{overall['avg_baths']:.2f}" if overall.get("avg_baths") else "—")

            st.caption("Overall calculado ponderando os ZIPs pelo volume (record_count).")
            st.dataframe(zip_table, use_container_width=True)

with tabs[1]:
    st.subheader("Tabela comparativa por ZIP")
    if not project_id:
        st.info("Importe um arquivo para gerar o project_id e começar as análises.")
    else:
        cat = st.selectbox("Categoria (ZIP Compare)", ["Listings", "Pendings", "Sold", "Land", "Rental"], index=2, key="zip_cat")
        df = fetch_project_data(engine, project_id, cat)

        if df.empty:
            st.info("Sem dados para esta categoria.")
        else:
            rows = []
            for z, g in df.groupby("zipcode"):
                m = compute_metrics_for_group(g)
                rows.append({"zipcode": z, **m})
            zip_table = pd.DataFrame(rows).sort_values(["record_count", "avg_price"], ascending=[False, False])
            st.dataframe(zip_table, use_container_width=True)

with tabs[2]:
    st.subheader("Sold Addresses (endereços vendidos)")
    if not project_id:
        st.info("Importe Sold para listar endereços.")
    else:
        df = fetch_project_data(engine, project_id, "Sold")
        if df.empty:
            st.info("Sem dados de Sold.")
        else:
            cols = ["sold_date", "zipcode", "address", "sold_price", "sqft", "beds", "baths", "adom", "sell_agent", "list_agent"]
            view = df[cols].copy()
            view["sold_date"] = pd.to_datetime(view["sold_date"], errors="coerce")
            view = view.sort_values("sold_date", ascending=False)
            st.dataframe(view, use_container_width=True)
