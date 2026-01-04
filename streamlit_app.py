import streamlit as st
import pandas as pd
from datetime import date
import plotly.express as px
from backend.etl import run_etl, get_engine
from backend.core.reports import MarketReports # Verifique se criou este arquivo
from backend.core.analyzer import MarketAnalyzer # Verifique se criou este arquivo

# Configuração da Página
st.set_page_config(page_title="Market Lens | AI Real Estate Insights", layout="wide")

# Estilização CSS para parecer um Dashboard Profissional
st.markdown("""
    <style>
    .main { background-color: #f5f7f9; }
    .stMetric { background-color: #ffffff; padding: 15px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
    </style>
    """, unsafe_allow_html=True)

# --- SIDEBAR ---
with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/3612/3612214.png", width=100)
    st.title("Market Lens AI")
    st.subheader("Configurações de Importação")
    
    uploaded_file = st.file_uploader("Upload MLS (XLSX ou CSV)", type=["xlsx", "csv"])
    snapshot_date = st.date_input("Data do Snapshot", date.today())
    contract_path = st.text_input("Contract YAML", "backend/contract/mls_column_contract.yaml")
    
    if st.button("🚀 Rodar Processamento ETL", use_container_width=True):
        if uploaded_file:
            with st.spinner("Processando dados e classificando com IA..."):
                result = run_etl(
                    xlsx_file=uploaded_file,
                    snapshot_date=snapshot_date,
                    contract_path=contract_path
                )
                if result.ok:
                    st.success(f"Sucesso! {result.rows_classified_inserted} imóveis processados.")
                    st.balloons()
                else:
                    st.error(f"Erro no ETL: {result.error}")
        else:
            st.warning("Por favor, suba um arquivo primeiro.")

# --- CONTEÚDO PRINCIPAL ---
st.title("📊 Dashboard de Inteligência Imobiliária")

# Inicializa classes de análise
reports = MarketReports()
analyzer = MarketAnalyzer()

try:
    # Carrega dados do banco para os relatórios
    df_master = reports.load_data()
    
    if df_master.empty:
        st.info("O banco de dados está vazio. Use a barra lateral para importar dados do MLS.")
    else:
        # Abas de Navegação
        tab_summary, tab_size, tab_year, tab_ai = st.tabs([
            "📈 Resumo do Mercado", 
            "🏠 Análise por Tamanho", 
            "📅 Análise por Ano/Preço", 
            "🤖 Assistente de IA"
        ])

        # TAB 1: RESUMO GERAL (Estilo Screenshot 15)
        with tab_summary:
            st.header("Overview de Inventário por ZIP Code")
            inventory = reports.get_inventory_overview(df_master)
            
            # Métricas em destaque
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total de Imóveis", len(df_master))
            c2.metric("Preço Médio", f"${df_master['list_price'].mean():,.2f}")
            c3.metric("Tamanho Médio", f"{df_master['heated_area'].mean():,.0f} sqft")
            c4.metric("ZIP Codes", df_master['zip'].nunique())
            
            st.dataframe(inventory, use_container_width=True)

        # TAB 2: ANÁLISE POR TAMANHO (Estilo Screenshot 11)
        with tab_size:
            st.header("House Size vs Zip Codes Analysis")
            size_analysis = reports.get_size_analysis(df_master)
            
            # Formatação para destacar valores altos
            st.dataframe(
                size_analysis.style.background_gradient(cmap='Blues', subset=size_analysis.columns[4:]),
                use_container_width=True
            )
            
            fig_size = px.scatter(df_master, x="heated_area", y="list_price", color="zip", 
                                 title="Correlação Preço vs Tamanho por ZIP")
            st.plotly_chart(fig_size, use_container_width=True)

        # TAB 3: ANÁLISE POR ANO (Estilo Screenshot 12)
        with tab_year:
            st.header("Building Year vs Price Range")
            year_analysis = reports.get_year_analysis(df_master)
            st.dataframe(year_analysis, use_container_width=True)
            
            # Gráfico de Tendência ADOM (Dias no Mercado)
            fig_adom = px.line(year_analysis, x="year_built", y="adom_medio", title="Média de Dias no Mercado por Ano de Construção")
            st.plotly_chart(fig_adom, use_container_width=True)

        # TAB 4: ASSISTENTE DE IA PARA INVESTIMENTOS
        with tab_ai:
            st.header("🤖 Market Lens AI Assistant")
            st.write("Analise oportunidades de investimento com base nos dados reais importados.")
            
            # Aqui buscamos os "Deals" (Imóveis abaixo do preço de mercado)
            deals = analyzer.find_undervalued_deals(threshold=0.90) # 10% abaixo da média
            
            if not deals.empty:
                st.subheader("🔥 Melhores Oportunidades Identificadas")
                st.write("Imóveis com preço por sqft significativamente abaixo da média do ZIP Code.")
                st.dataframe(deals[['ml_number', 'address', 'zip', 'list_price', 'price_sqft', 'avg_price_sqft', 'deal_score']], use_container_width=True)
                
                # Interface de Chat (Simulada para Gemini)
                query = st.text_input("Pergunte à IA sobre estes imóveis (ex: Qual o melhor ROI aqui?)")
                if query:
                    # Aqui você chamaria sua função ai/gemini_ai.py
                    st.write(f"**Análise da IA:** Com base no ADOM de {deals['adom'].iloc[0]} dias e no preço 15% abaixo da média de {deals['zip'].iloc[0]}, este imóvel é uma excelente oportunidade para Flip.")
            else:
                st.write("Nenhum 'deal' óbvio encontrado no momento. Tente importar mais dados.")

except Exception as e:
    st.error(f"Erro ao carregar dashboard: {e}")
    st.info("Certifique-se de que o banco de dados está configurado e o ETL foi rodado pelo menos uma vez.")
