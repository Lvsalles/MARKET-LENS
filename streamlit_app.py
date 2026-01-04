import streamlit as st
from datetime import date

st.set_page_config(page_title="Market Lens — MLS ETL", layout="centered")

st.title("Market Lens — MLS ETL")
st.success("Backend carregado com sucesso")

# --- Upload ---
uploaded_file = st.file_uploader(
    "Upload de arquivos MLS (.xlsx)",
    type=["xlsx"]
)

snapshot_date = st.date_input(
    "Snapshot date",
    value=date.today()
)

# --- Botão ETL ---
if st.button("Executar ETL"):
    if uploaded_file is None:
        st.error("Por favor, selecione um arquivo XLSX antes de executar o ETL.")
    else:
        st.info("Executando ETL...")
        try:
            from backend.etl import run_etl

            result = run_etl(
                uploaded_file,
                snapshot_date
            )

            st.success("ETL executado com sucesso!")
            st.json(result)

        except Exception as e:
            st.error("Erro ao executar ETL")
            st.exception(e)
