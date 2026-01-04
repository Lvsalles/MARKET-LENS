# streamlit_app.py
from __future__ import annotations

import tempfile
from datetime import date
from pathlib import Path

import streamlit as st

from backend.etl import run_etl


# =========================================================
# Configuração da aplicação
# =========================================================

APP_TITLE = "Market Lens — ETL Upload"

# 🔴 CONTRATO REAL (confirmado pelo repo)
DEFAULT_CONTRACT_PATH = "backend/contract/mls_column_contract.yaml"


# =========================================================
# Helpers
# =========================================================

def save_uploaded_file(uploaded_file) -> Path:
    """
    Salva o arquivo enviado pelo Streamlit em um arquivo temporário
    e retorna o Path para uso no ETL.
    """
    suffix = Path(uploaded_file.name).suffix or ".xlsx"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(uploaded_file.getbuffer())
        return Path(tmp.name)


# =========================================================
# App principal
# =========================================================

def main() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)

    st.info(
        "Env vars necessárias no Streamlit Cloud:\n"
        "- DATABASE_URL (ou SUPABASE_DB_URL)\n\n"
        "Este app salva o XLSX em arquivo temporário e executa o ETL "
        "de forma resiliente ao schema do banco."
    )

    # -----------------------------
    # Inputs
    # -----------------------------
    col1, col2 = st.columns(2)

    with col1:
        uploaded_file = st.file_uploader(
            "Upload do arquivo MLS (.xlsx)",
            type=["xlsx"],
        )

        snapshot_date = st.date_input(
            "Snapshot date",
            value=date.today(),
        )

    with col2:
        contract_path = st.text_input(
            "Contract path (YAML)",
            value=DEFAULT_CONTRACT_PATH,
        )

    # Validação antecipada do contrato
    contract_file = Path(contract_path)
    if not contract_file.exists():
        st.error(f"Contract YAML não encontrado em: {contract_file}")
        st.stop()

    # -----------------------------
    # Execução
    # -----------------------------
    run_button = st.button(
        "Rodar ETL",
        type="primary",
        disabled=uploaded_file is None,
    )

    if run_button:
        st.write("Iniciando ETL...")

        if uploaded_file is None:
            st.error("Nenhum arquivo foi enviado.")
            return

        try:
            tmp_xlsx = save_uploaded_file(uploaded_file)
            st.write(f"Arquivo temporário: {tmp_xlsx.name}")

            result = run_etl(
                tmp_xlsx,
                contract_path=contract_file,
                snapshot_date=snapshot_date,
            )

            if result.ok:
                st.success("ETL finished successfully!")
                st.json(
                    {
                        "ok": True,
                        "import_id": result.import_id,
                        "inserted_raw": result.inserted_raw,
                        "inserted_classified_rows": result.inserted_classified_rows,
                    }
                )
            else:
                st.error("Erro ao executar ETL")
                st.json(
                    {
                        "ok": False,
                        "error": result.error,
                    }
                )

        except Exception as e:
            st.error("Erro inesperado na execução do ETL")
            st.exception(e)


# =========================================================
# Entry point
# =========================================================

if __name__ == "__main__":
    main()
