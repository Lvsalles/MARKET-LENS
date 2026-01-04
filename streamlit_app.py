import streamlit as st
import tempfile
from pathlib import Path

from backend.etl import run_etl

st.set_page_config(page_title="Market Lens — MLS ETL", layout="centered")

st.title("Market Lens — MLS ETL")

uploaded_file = st.file_uploader(
    "Upload de arquivos MLS (.xlsx)",
    type=["xlsx"],
)

contract_path = Path("backend/contracts/mls_contract.yml")

if uploaded_file:
    st.info("Iniciando ETL...")

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(uploaded_file.read())
            tmp_path = Path(tmp.name)

        st.write(f"Arquivo temporário: {tmp_path.name}")

        result = run_etl(
            xlsx_path=tmp_path,
            contract_path=contract_path,
        )

        st.success("ETL finalizado com sucesso!")
        st.json(result)

    except Exception as e:
        st.error("Erro inesperado na execução do ETL")
        st.exception(e)
