import streamlit as st
from datetime import date
from pathlib import Path

from backend.etl import run_etl

st.title("Market Lens — MLS Import")

uploaded = st.file_uploader("Upload MLS XLSX", type=["xlsx"])
snapshot = st.date_input("Snapshot date", value=date.today())

# ajuste o path do contrato conforme seu projeto:
# exemplo: backend/contracts/mls_contract.yaml
CONTRACT_PATH = Path("backend/contracts/mls_contract.yaml")

run_btn = st.button("Run ETL")

if run_btn:
    if not uploaded:
        st.error("Please upload an XLSX file.")
        st.stop()

    try:
        result = run_etl(
            xlsx_path=uploaded,                 # UploadedFile OK
            contract_path=str(CONTRACT_PATH),   # str|Path OK
            snapshot_date=snapshot,             # date OK
        )
        st.success("ETL finished successfully!")
        st.json(result)

    except Exception as e:
        st.error("Erro ao executar ETL")
        st.exception(e)
