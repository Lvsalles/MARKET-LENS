import streamlit as st
import pandas as pd
import numpy as np
from datetime import date
import plotly.express as px
from backend.etl import run_etl
from backend.core.reports import MarketReports
from backend.core.analyzer import MarketAnalyzer

st.set_page_config(page_title="Market Lens | AI Real Estate", layout="wide")

# Inicialização
reports = MarketReports()
analyzer = MarketAnalyzer()

# Sidebar e ETL (Mantido igual ao anterior)
with st.sidebar:
    st.title("Market Lens AI")
    uploaded_file = st.file_uploader("Upload MLS (XLSX ou CSV)", type=["xlsx", "csv"])
    snapshot_date = st.date_input("Data do Snapshot", date.today())
    if st.button("🚀 Rodar ETL"):
        if uploaded_file:
            result = run_etl(xlsx_file=uploaded_file, snapshot_date=snapshot_date, contract_path="backend/contract/mls_column_contract.yaml")
            if result.ok: st.success("Processado!")
            else: st.error(result.error)

# --- DASHBOARD ---
st.title("📊 Dashboard de Inteligência Imobiliária")

try:
    df_master = reports.load_data()
    if not df_master.empty:
        t1, t2, t3, t4, t5 = st.tabs(["Resumo", "Tamanho", "Ano/Preço", "Mensal (MoM)", "🤖 IA Assistant"])

        with t1:
            st.dataframe(reports.get_inventory_overview(df_master), use_container_width=True)

        with t2:
            st.subheader("House Size vs Zip Codes")
            df_size = reports.get_size_analysis(df_master)
            # Formatação de Moeda e Gradiente
            st.dataframe(df_size.style.format({"VALOR MÉDIO": "${:,.2f}", "$/SQFT": "${:,.2f}"}).background_gradient(cmap='YlGnBu'), use_container_width=True)

        with t3:
            st.subheader("Building Year vs Price Range")
            df_year = reports.get_year_analysis(df_master)
            st.dataframe(df_year.style.format({"VALOR MÉDIO": "${:,.2f}", "$/SQFT": "${:,.2f}"}), use_container_width=True)

        with t4:
            st.subheader("Month over Month Analysis (MoM)")
            df_mom = reports.get_mom_analysis(df_master)
            st.dataframe(df_mom.style.format({"VALOR MÉDIO": "${:,.2f}", "$/SQFT": "${:,.2f}"}), use_container_width=True)

        with t5:
            st.subheader("🤖 Sugestões de Investimento IA")
            deals = analyzer.find_undervalued_deals()
            if not deals.empty:
                st.write("Imóveis identificados abaixo da média do bairro:")
                st.dataframe(deals[['ml_number', 'address', 'zip', 'list_price', 'deal_score']])
            else:
                st.write("Aguardando mais dados para análise de ROI.")

except Exception as e:
    st.error(f"Erro: {e}")
