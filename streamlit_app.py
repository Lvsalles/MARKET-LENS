# ============================================================
# Market Lens — MLS Import (Streamlit)
# Cloud-first, resilient, deterministic
# ============================================================

from __future__ import annotations

import tempfile
from datetime import date
from pathlib import Path

import streamlit as st

# ============================================================
# Safe backend import
# ============================================================

try:
    from backend.etl import run_etl
    BACKEND_OK = True
except Exception as e:
    BACKEND_OK = False
    BACKEND_ERROR = e


# ============================================================
# Streamlit Page Config
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
else:
    st.success("Backend carregado com sucesso")


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
        # Persist uploaded file to temp path (CRITICAL FIX)
        # ----------------------------------------------------
        with tempfile.NamedTemporaryFile(
            delete=False,
            suffix=".xlsx"
        ) as tmp:
            tmp.write(uploaded_file.getbuffer())
            tmp_path = Path(tmp.name)

        # ----------------------------------------------------
        # Run ETL
        # ----------------------------------------------------
        result = run_etl(
            xlsx_path=tmp_path,
            snapshot_date=snapshot_date,
        )

        st.success("ETL finished successfully!")

        # ----------------------------------------------------
        # Optional result rendering (SAFE)
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
