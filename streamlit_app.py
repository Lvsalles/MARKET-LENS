import streamlit as st
from datetime import date
from pathlib import Path

from backend.etl import run_etl

# =========================================================
# UI
# =========================================================

st.set_page_config(page_title="Market Lens — MLS ETL", layout="centered")
st.title("Market Lens — MLS Import")

uploaded = st.file_uploader("Upload MLS XLSX", type=["xlsx"])
snapshot = st.date_input("Snapshot date", value=date.today())

# ajuste se o caminho do contrato for diferente
CONTRACT_PATH = Path("backend/contracts/mls_contract.yaml")

run_btn = st.button("Run ETL")

# =========================================================
# ACTION
# =========================================================

if run_btn:
    if not uploaded:
        st.error("Please upload an XLSX file.")
        st.stop()

    try:
        result = run_etl(
            xlsx_path=uploaded,
            contract_path=str(CONTRACT_PATH),
            snapshot_date=snapshot,
        )

        st.success("ETL finished successfully!")
st.markdown(
    f"""
    **Import ID:** `{result.get("import_id")}`  
    **Rows inserted:** `{result.get("rows_inserted")}`
    """
)
