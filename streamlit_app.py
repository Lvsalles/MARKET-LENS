# ============================================================
# Market Lens — MLS Import (Streamlit)
# ============================================================

from __future__ import annotations

import tempfile
from datetime import date
from pathlib import Path

import streamlit as st


# ============================================================
# Backend import (safe)
# ============================================================

try:
    from backend.etl import run_etl
    BACKEND_OK = True
except Exception as e:
    BACKEND_OK = False
    BACKEND_ERROR = e


# ============================================================
# Page config
# ============================================================

st.set_page_config(
    page_title="Market Lens — MLS Import",
    layout="centered",
)

st.title("Market Lens — MLS Import")


# ============================================================
# Backend status
# ============================================================

if not BACKEND_OK:
    st.error("❌ Backend não pôde ser carregado")
    st.exception(BACKEND_ERROR)
    st.stop()

st.success("Backend carregado com sucesso")


# ============================================================
# FIXO E OBRIGATÓRIO — CONTRACT PATH
# ============================================================

CONTRACT_PATH = Path("backend/contracts/mls_column_contract.yaml")

if not CONTRACT_PATH.exists():
    st.error(f"Contrato não encontrado: {CONTRACT_PATH}")
    st.stop()


# ============================================================
# Upload UI
# ============================================================

uploaded_file = st.file_uploader(
    "Upload MLS XLSX",
    type=["xlsx"],
)

snapshot_date = st.date_input(
    "Snapshot date",
    value=date.today(),
)


# ============================================================
# Run ETL
# ============================================================

if st.button("Run ETL"):

    if not uploaded_file:
        st.warning("Selecione um arquivo XLSX primeiro.")
        st.stop()

    try:
        # ----------------------------------------------------
        # Persist uploaded file to temp path (CRÍTICO)
        # ----------------------------------------------------
        with tempfile.NamedTemporaryFile(
            delete=False,
            suffix=".xlsx"
        ) as tmp:
            tmp.write(uploaded_file.getbuffer())
            xlsx_path = Path(tmp.name)

        # ----------------------------------------------------
        # Run ETL (CONTRATO COMPLETO)
        # ----------------------------------------------------
        result = run_etl(
            xlsx_path=xlsx_path,
            snapshot_date=snapshot_date,
            contract_path=CONTRACT_PATH,
        )

        st.success("ETL finished successfully!")

        # ----------------------------------------------------
        # Render result safely
        # ----------------------------------------------------
        if isinstance(result, dict):
            st.markdown(
                f"""
                **Import ID:** `{result.get("import_id")}`  
                **Rows inserted:** `{result.get("rows_inserted")}`
                """
            )
        else:
            st.write(result)

    except Exception as e:
        st.error("Erro ao executar ETL")
        st.exception(e)
