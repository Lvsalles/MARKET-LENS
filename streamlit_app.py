from __future__ import annotations

import tempfile
from datetime import date
from pathlib import Path

import streamlit as st

try:
    from backend.etl import run_etl
    BACKEND_OK = True
except Exception as e:
    BACKEND_OK = False
    BACKEND_ERROR = e

st.set_page_config(page_title="Market Lens — MLS Import", layout="centered")
st.title("Market Lens — MLS Import")

if not BACKEND_OK:
    st.error("❌ Backend não pôde ser carregado")
    st.exception(BACKEND_ERROR)
    st.stop()

st.success("Backend carregado com sucesso")

CONTRACT_PATH = Path("backend/contracts/mls_column_contract.yaml")
if not CONTRACT_PATH.exists():
    st.error(f"Contrato não encontrado: {CONTRACT_PATH}")
    st.stop()

uploaded_file = st.file_uploader("Upload MLS XLSX", type=["xlsx"])
snapshot_date = st.date_input("Snapshot date", value=date.today())

colA, colB = st.columns(2)
with colA:
    want_preview = st.checkbox("Mostrar preview (top 50)", value=True)
with colB:
    preview_rows = 50 if want_preview else 0

if st.button("Run ETL"):
    if not uploaded_file:
        st.warning("Selecione um arquivo XLSX primeiro.")
        st.stop()

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(uploaded_file.getbuffer())
            xlsx_path = Path(tmp.name)

        result = run_etl(
            xlsx_path=xlsx_path,
            snapshot_date=snapshot_date,
            contract_path=CONTRACT_PATH,
            preview_rows=preview_rows,
        )

        st.success("ETL finished successfully!")

        st.markdown(
            f"""
            **Import ID:** `{result.get("import_id")}`  
            **Filename:** `{result.get("filename")}`  
            **Snapshot date:** `{result.get("snapshot_date")}`  
            **Asset class:** `{result.get("asset_class")}`  
            **Rows classified:** `{result.get("rows_classified")}`  
            **Rows inserted:** `{result.get("rows_inserted")}`  
            """
        )

        st.subheader("Summary")
        st.json(result.get("summary", {}))

        if want_preview:
            st.subheader("Preview (top 50)")
            preview = result.get("preview", [])
            if preview:
                st.dataframe(preview, use_container_width=True)
            else:
                st.info("Sem preview disponível.")

    except Exception as e:
        st.error("Erro ao executar ETL")
        st.exception(e)
