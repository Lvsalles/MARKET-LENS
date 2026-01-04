# streamlit_app.py
from __future__ import annotations

import tempfile
from datetime import date
from pathlib import Path

import streamlit as st

# Backend imports
from backend.etl import run_etl


APP_TITLE = "Market Lens — ETL Upload"


def _save_uploaded_file_to_temp(uploaded_file) -> Path:
    suffix = Path(uploaded_file.name).suffix.lower() or ".xlsx"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(uploaded_file.getbuffer())
        return Path(tmp.name)


def main() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)

    st.info(
        "Env vars necessárias no Streamlit Cloud:\n"
        "- DATABASE_URL (ou SUPABASE_DB_URL)\n"
        "\nEste app salva o XLSX em temp e roda o ETL com inserção resiliente ao schema."
    )

    col1, col2 = st.columns(2)

    with col1:
        uploaded = st.file_uploader("Upload do arquivo MLS (.xlsx)", type=["xlsx"])
        snapshot = st.date_input("Snapshot date", value=date.today())

    with col2:
        # Ajuste este caminho para onde seu contrato YAML está no repo
        default_contract = "backend/contracts/mls_contract.yml"
        contract_path = st.text_input("Contract path (YAML)", value=default_contract)

    run_btn = st.button("Rodar ETL", type="primary", disabled=(uploaded is None))

    if run_btn:
        st.write("Iniciando ETL...")
        if uploaded is None:
            st.error("Nenhum arquivo enviado.")
            return

        try:
            tmp_path = _save_uploaded_file_to_temp(uploaded)
            st.write(f"Arquivo temporário: `{tmp_path.name}`")

            result = run_etl(
                tmp_path,
                contract_path=contract_path,
                snapshot_date=snapshot,
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
                st.json({"ok": False, "error": result.error})

        except Exception as e:
            st.error("Erro inesperado no Streamlit")
            st.exception(e)


if __name__ == "__main__":
    main()
