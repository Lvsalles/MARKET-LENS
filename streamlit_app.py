import streamlit as st
import pandas as pd

from db import get_engine
from etl import detect_and_map, insert_into_staging
from sqlalchemy import text

st.set_page_config(page_title="Market Lens", layout="wide", page_icon="📊")
st.title("📊 Market Lens — Market Intelligence")

# =========================
# DB CONNECT
# =========================
try:
    engine = get_engine()
    st.success("Banco conectado com sucesso ✅")
except Exception as e:
    st.error("Erro ao conectar ao banco")
    st.code(str(e))
    st.stop()

# =========================
# SIDEBAR
# =========================
with st.sidebar:
    st.header("Configurações")
    project_id = st.text_input("Project ID", value="default_project")
    data_type = st.selectbox("Tipo de dados", ["Properties (ACT/PND/SLD juntos)", "Land", "Rental"])
    st.caption("Dica: Properties pode conter ACT/PND/SLD no mesmo arquivo.")

tabs = st.tabs(["Upload", "Diagnostics", "Overview"])

# =========================
# TAB 1 — UPLOAD
# =========================
with tabs[0]:
    st.subheader("📥 Upload (até 12 arquivos .xlsx)")

    files = st.file_uploader(
        "Arraste e solte os arquivos XLSX aqui",
        type=["xlsx"],
        accept_multiple_files=True
    )

    if files and len(files) > 12:
        st.error("Máximo 12 arquivos por upload.")
        st.stop()

    if files:
        st.info(f"{len(files)} arquivo(s) selecionado(s).")

        if st.button("Processar e salvar no banco"):
            total_inserted = 0
            total_skipped = 0

            with st.spinner("Processando arquivos..."):
                for f in files:
                    df_raw = pd.read_excel(f)
                    df_canon = detect_and_map(df_raw)

                    stats = insert_into_staging(engine, df_canon, project_id=project_id, source_file=f.name)

                    total_inserted += stats["inserted"]
                    total_skipped += stats["skipped_duplicates"]

                    st.write(f"✅ {f.name} — inserted: {stats['inserted']} | duplicates skipped: {stats['skipped_duplicates']}")

            st.success(f"Concluído. Inseridos: {total_inserted} | Duplicados ignorados: {total_skipped}")

# =========================
# TAB 2 — DIAGNOSTICS
# =========================
with tabs[1]:
    st.subheader("🧪 Diagnostics (qualidade + conferência)")

    q = text("""
        SELECT status_norm, COUNT(*) as rows
        FROM stg_mls
        WHERE project_id = :project_id
        GROUP BY 1
        ORDER BY rows DESC
    """)
    with engine.begin() as conn:
        df_counts = pd.read_sql(q, conn, params={"project_id": project_id})

    st.write("Distribuição por status_norm (SOLD/ACTIVE/PENDING/RENTAL/LAND):")
    st.dataframe(df_counts, use_container_width=True)

    q2 = text("""
        SELECT COUNT(*) AS total_rows
        FROM stg_mls
        WHERE project_id = :project_id
    """)
    with engine.begin() as conn:
        total = conn.execute(q2, {"project_id": project_id}).fetchone()[0]

    st.metric("Total de linhas no projeto", int(total))

# =========================
# TAB 3 — OVERVIEW
# =========================
with tabs[2]:
    st.subheader("📈 Overview — Investor Grade (base)")

    q = text("""
        SELECT *
        FROM stg_mls
        WHERE project_id = :project_id
    """)
    with engine.begin() as conn:
        df = pd.read_sql(q, conn, params={"project_id": project_id})

    if df.empty:
        st.warning("Nenhum dado no banco para este project_id. Vá em Upload e processe arquivos.")
        st.stop()

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total", len(df))
    c2.metric("Sold", int((df["status_norm"] == "SOLD").sum()))
    c3.metric("Active", int((df["status_norm"] == "ACTIVE").sum()))
    c4.metric("Pending", int((df["status_norm"] == "PENDING").sum()))
    c5.metric("Land/Rental", int(((df["status_norm"] == "LAND") | (df["status_norm"] == "RENTAL")).sum()))

    st.markdown("### Preview (últimas 200 linhas)")
    st.dataframe(df.tail(200), use_container_width=True)
