import streamlit as st
import pandas as pd
from datetime import date
from backend.etl import run_etl
from backend.core.reports import MarketReports

st.set_page_config(page_title="Market Lens", layout="wide")
st.title("📊 Dashboard de Inteligência Imobiliária")

reports = MarketReports()

with st.sidebar:
    st.header("Importação")
    file = st.file_uploader("Upload MLS", type=["xlsx", "csv"])
    dt = st.date_input("Data", date.today())
    if st.button("Rodar ETL"):
        if file:
            res = run_etl(xlsx_file=file, snapshot_date=dt, contract_path="backend/contract/mls_column_contract.yaml")
            if res.ok: st.success("Processado!")
            else: st.error(res.error)

try:
    df = reports.load_data()
    if not df.empty:
        t1, t2, t3, t4 = st.tabs(["Resumo", "Tamanho", "Ano/Preço", "MoM"])
        with t1: st.dataframe(reports.get_inventory_overview(df), use_container_width=True)
        with t2: st.dataframe(reports.get_size_analysis(df).style.format({"VALOR MÉDIO": "${:,.2f}", "$/SQFT": "${:,.2f}"}), use_container_width=True)
        with t3: st.dataframe(reports.get_year_analysis(df).style.format({"VALOR MÉDIO": "${:,.2f}", "$/SQFT": "${:,.2f}"}), use_container_width=True)
        with t4: st.dataframe(reports.get_mom_analysis(df).style.format({"VALOR MÉDIO": "${:,.2f}", "$/SQFT": "${:,.2f}"}), use_container_width=True)
    else:
        st.info("Banco vazio.")
except Exception as e:
    st.error(f"Erro no Dashboard: {e}")
