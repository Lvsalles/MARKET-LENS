"""
Market Lens — Streamlit ETL Runner (Cloud-first)

Função:
- Interface simples para rodar o ETL
- Upload de arquivos MLS (.xlsx)
- Visualizar resultado básico
"""

import streamlit as st
import pandas as pd
from datetime import date
from pathlib import Path
import tempfile

from backend.etl import run_etl


# -------------------------------------------------
# Page config
# -------------------------------------------------
st.set_page_config(
    page_title="Market Lens — ETL",
    layout="wide",
)


# -------------------------------------------------
# Header
# -------------------------------------------------
st.title("🏗️ Market Lens — MLS ETL")
st.caption("Upload de arquivos MLS (.xlsx) e ingestão no banco")


# -------------------------------------------------
# File upload
# -------------------------------------------------
uploaded_files = st.file_uploader(
    "Selecione um ou mais arquivos XLSX",
    type=["xlsx"],
    accept_multiple_files=True,
)

run_button = st.button("▶️ Rodar ETL")


# -------------------------------------------------
# Run ETL
# -------------------------------------------------
if run_button and uploaded_files:
    with st.spinner("Processando arquivos..."):
        temp_paths = []

        try:
            # salvar arquivos temporários (necessário no Streamlit)
            for f in uploaded_files:
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
                tmp.write(f.read())
                tmp.close()
                temp_paths.append(tmp.name)

            # executar ETL
            df = run_etl(
                xlsx_files=temp_paths,
                snapshot_date=date.today(),
                persist=True,
            )

            st.success("ETL executado com sucesso!")

            # -------------------------------------------------
            # Summary
            # -------------------------------------------------
            st.subheader("Resumo por Asset Class e Status")
            summary = (
                df.groupby(["asset_class", "status_group"])
                .size()
                .reset_index(name="total")
            )
            st.dataframe(summary, use_container_width=True)

            # -------------------------------------------------
            # Preview
            # -------------------------------------------------
            st.subheader("Preview dos dados ingeridos")
            st.dataframe(df.head(50), use_container_width=True)

        except Exception as e:
            st.error(f"Erro ao executar ETL: {e}")

else:
    st.info("Faça upload de pelo menos um arquivo XLSX para iniciar.")
