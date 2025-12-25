import streamlit as st
import pandas as pd
import numpy as np
import google.generativeai as genai
from sklearn.linear_model import LinearRegression
import folium
from streamlit_folium import folium_static

# 1. SETUP INICIAL
st.set_page_config(page_title="AI Predictive Investor", layout="wide")
genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])

# 2. BIBLIOTECA DE ZONEAMENTO (ADU VIABILITY)
# Mapeamento de leis locais de North Port / Venice
ZONING_ADU_RULES = {
    'RSF1': 'Low Potential (Strict setbacks)',
    'RSF2': 'High Potential (ADU allowed with permit)',
    'RSF3': 'Moderate Potential',
    'RMH': 'Mobile Home Zone (Check park rules)',
    'AG': 'High Potential (Acreage allows ADUs)'
}

# 3. MOTOR DE NORMALIZAÇÃO
def advanced_normalize(df):
    mapping = {
        'Price': ['Current Price', 'Price', 'List Price', 'Sold Price'],
        'SqFt': ['Heated Area', 'Heated Area_num', 'SqFt'],
        'Beds': ['Beds', 'Bedrooms', 'Beds_num'],
        'Baths': ['Full Baths', 'Bathrooms', 'Full Baths_num'],
        'Zip': ['Zip', 'Zip Code'],
        'Zoning': ['Zoning', 'Zoning Code']
    }
    for std, syns in mapping.items():
        found = next((c for c in syns if c in df.columns), None)
        if found: df = df.rename(columns={found: std})
    
    # Limpeza e conversão
    if 'Price' in df.columns:
        df['Price'] = pd.to_numeric(df['Price'].astype(str).str.replace(r'[$,]', '', regex=True), errors='coerce')
    if 'SqFt' in df.columns:
        df['Price_SqFt'] = df['Price'] / df['SqFt']
    
    return df.dropna(subset=['Price', 'SqFt', 'Beds'])

# 4. MOTOR DE REGRESSÃO (PREVISÃO DE PREÇO JUSTO)
def find_undervalued_assets(df):
    if len(df) < 10: return df
    
    # Preparar variáveis para o modelo
    X = df[['SqFt', 'Beds', 'Baths']].fillna(0)
    y = df['Price']
    
    model = LinearRegression()
    model.fit(X, y)
    
    # Prever Preço Justo e calcular Residual (Diferença)
    df['Fair_Value'] = model.predict(X)
    df['Arbitrage_Potential'] = df['Fair_Value'] - df['Price']
    df['Opportunity_Score'] = (df['Arbitrage_Potential'] / df['Price']) * 100
    
    return df.sort_values(by='Opportunity_Score', ascending=False)

# ---------------------------------------------------------
# UI TERMINAL
# ---------------------------------------------------------
st.title("🏙️ Predictive Investment Terminal")
st.sidebar.header("🎯 Parâmetros de Filtro")

uploaded_files = st.file_uploader("Upload MLS Data (CSV/XLSX)", accept_multiple_files=True)

if uploaded_files:
    dfs = []
    for f in uploaded_files:
        df = pd.read_csv(f) if f.name.endswith('.csv') else pd.read_excel(f)
        dfs.append(advanced_normalize(df))
    
    if dfs:
        main_df = pd.concat(dfs, ignore_index=True)
        
        # Filtros Dinâmicos
        min_beds = st.sidebar.slider("Min Bedrooms", 1, 5, 3)
        max_price = st.sidebar.number_input("Max Budget", value=500000)
        
        filtered_df = main_df[(main_df['Beds'] >= min_beds) & (main_df['Price'] <= max_price)]
        
        # Executar Inteligência Preditiva
        scored_df = find_undervalued_assets(filtered_df)

        # SEÇÃO 1: OPORTUNIDADES DE ARBITRAGEM
        st.subheader("💎 Top Arbitrage Opportunities (Undervalued)")
        st.dataframe(scored_df[['Address', 'Price', 'Fair_Value', 'Opportunity_Score', 'Zip']].head(10))
        st.caption("Nota: 'Fair Value' é calculado via Regressão Multivariada baseada nos seus dados.")

        # SEÇÃO 2: VIABILIDADE DE ADU
        if 'Zoning' in scored_df.columns:
            st.subheader("🏗️ ADU Feasibility (Construction Potential)")
            scored_df['ADU_Potential'] = scored_df['Zoning'].map(ZONING_ADU_RULES).fillna('Unknown')
            adu_hits = scored_df[scored_df['ADU_Potential'].str.contains('High')]
            st.write(f"Encontradas **{len(adu_hits)}** propriedades com alto potencial de construção extra.")
            st.dataframe(adu_hits[['Address', 'Zoning', 'ADU_Potential', 'Price']])

        # SEÇÃO 3: RELATÓRIO ESTRATÉGICO IA
        if st.button("🚀 Gerar Due Diligence AI Report"):
            with st.spinner('AI analisando riscos e viabilidade financeira...'):
                # Resumo para a IA focar no ROI
                top_opportunity = scored_df.iloc[0].to_dict() if not scored_df.empty else {}
                
                prompt = f"""
                Você é um consultor de Due Diligence da Deloitte. 
                Analise esta oportunidade de topo: {top_opportunity}
                
                Cruze com:
                1. Taxas de juros atuais (6-7%) e impacto no ROI.
                2. Viabilidade de construir uma ADU (Guest House) nesta zona.
                3. Tendências PWC para o mercado de Sarasota County em 2025.
                4. Recomendação de "Exit Strategy" (Flip vs Build-to-Rent).
                """
                
                model = genai.GenerativeModel('gemini-1.5-flash')
                response = model.generate_content(prompt)
                st.markdown(response.text)
