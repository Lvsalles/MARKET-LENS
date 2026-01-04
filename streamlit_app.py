from __future__ import annotations

from datetime import date
from pathlib import Path

import streamlit as st

# ✅ IMPORT SEGURO (se quebrar, você vê na tela)
try:
    from backend.etl import run_etl
except Exception as e:
    st.set_page_config(page_title="Market Lens — ERROR", layout="centered")
    st.title("Backend não pôde ser carregado")
    st.exception(e)
    st.stop()


def main():
    st.set_page_config(page_title="Market Lens — ETL Upload", layout="wide")
    st.title("Market Lens — ETL Upload")

    st.info(
        "Env vars necessárias no Streamlit Cloud:\n\n"
        "- DATABASE_URL (ou SUPABASE_DB_URL)\n\n"
        "Este app salva o XLSX em temp e roda o ETL com inserção resiliente ao schema."
    )

    col1, col2 = st.columns([2, 1])

    with col1:
        uploaded = st.file_uploader("Upload do arquivo MLS (.xlsx)", type=["xlsx"])

    # ✅ Seu repo é backend/contract (singular) e o YAML chama mls_column_contract.yaml
    default_contract = "backend/contract/mls_column_contract.yaml"

    with col2:
        contract_path = st.text_input("Contract path (YAML)", value=default_contract)

    snapshot_date = st.date_input("Snapshot date", value=date.today())

    if uploaded is None:
        st.warning("Envie um XLSX para iniciar.")
        return

    st.caption(f"Arquivo: {uploaded.name}")

    if st.button("Rodar ETL", type="primary"):
        st.write("Iniciando ETL...")

        # Validação do contrato (antes do ETL)
        if not Path(contract_path).exists():
            st.error(f"Contract not found: {contract_path}")
            st.stop()

        with st.spinner("Executando ETL..."):
            result = run_etl(
                xlsx_file=uploaded,
                snapshot_date=snapshot_date,
                contract_path=contract_path,
            )

        if result.ok:
            st.success("ETL finished successfully!")
            st.json(
                {
                    "ok": result.ok,
                    "import_id": result.import_id,
                    "rows_raw_inserted": result.rows_raw_inserted,
                    "rows_classified_inserted": result.rows_classified_inserted,
                }
            )
        else:
            st.error("Erro ao executar ETL")
            st.json({"ok": False, "error": result.error})


if __name__ == "__main__":
    main()
