import streamlit as st
import pandas as pd
import numpy as np
import google.generativeai as genai
from sklearn.linear_model import LinearRegression
import folium
from streamlit_folium import folium_static

# 1. SETUP INICIAL
st.set_page_config(page_title="AI Predictive Investor", layout="wide")

if "GOOGLE_API_KEY" in st.secrets:
    genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])
else:
    st.error("🔑 API Key não encontrada.")

# 2. BIBLIOTECA DE PADRONIZAÇÃO (Baseada nos seus arquivos reais)
SYNONYMS = {
    'Price': ['Current Price_num', 'Current Price', 'List Price', 'Price'],
    'SqFt': ['Heated Area_num', 'Heated Area', 'SqFt', 'Lot Size Square Footage_num'],
    'Beds': ['Beds_num', 'Beds', 'Bedrooms'],
    'Baths': ['Full Baths_num', 'Full Baths', 'Bathrooms'],
    'Zip': ['Zip', 'Zip Code'],
    'Zoning': ['Zoning', 'Zoning Code']
}

def robust_normalize(df):
    # Renomear colunas encontradas
    for std, syns in SYNONYMS.items():
        found = next((c for c in syns if c in df.columns), None)
        if found:
            df = df.rename(columns={found: std})
    
    # Garantir que as colunas essenciais existam (mesmo que vazias)
    for col in ['Price', 'SqFt', 'Beds', 'Baths']:
        if col not in df.columns:
            df[col] = np.nan
            
    # Converter para numérico e limpar
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
    df['SqFt'] = pd.to_numeric(df['SqFt'], errors='coerce')
    
    # Remove apenas se houver dados mínimos para análise
    return df.dropna(subset=['Price'])

# 3. MOTOR DE ARBITRAGEM (REGRESSÃO)
def calculate_arbitrage(df):
    # Filtra apenas registros com dados completos para o modelo
    model_df = df.dropna(subset=['Price', 'SqFt', 'Beds', 'Baths']).copy()
    if len(model_df) < 5:
        return df
    
    X = model_df[['SqFt', 'Beds', 'Baths']]
    y = model_df['Price']
    
    model = LinearRegression().fit(X, y)
    df['Fair_Value'] = model.predict(df[['SqFt', 'Beds', 'Baths']].fillna(0))
    df['Opportunity_Score'] = ((df['Fair_Value'] - df['Price']) / df['Price']) * 100
    return df

# 4. INTERFACE
st.title("🏙️ Predictive Investment Terminal")
st.sidebar.header("🎯 Parâmetros de Filtro")

files = st.file_uploader("Upload MLS Data (CSV/XLSX)", accept_multiple_files=True)

if files:
    all_data = []
    for f in files:
        df = pd.read_csv(f) if f.name.endswith('.csv') else pd.read_excel(f)
        all_data.append(robust_normalize(df))
    
    if all_data:
        main_df = pd.concat(all_data, ignore_index=True)
        
        # Executar análise preditiva
        scored_df = calculate_arbitrage(main_df)
        
        # EXIBIÇÃO DE RESULTADOS
        st.subheader("💎 Oportunidades de Arbitragem Identificadas")
        st.write("Propriedades cujo preço está abaixo da tendência estatística do mercado local.")
        st.dataframe(scored_df.sort_values(by='Opportunity_Score', ascending=False).head(15))

        # IA ANALYTICS
        if st.button("🚀 Gerar Relatório de Estratégia"):
            model = genai.GenerativeModel('gemini-1.5-flash')
            # Resumo estatístico para a IA
            stats = scored_df.describe().to_string()
            prompt = f"Como especialista em Real Estate, analise estes padrões de preço e arbitragem: {stats}"
            response = model.generate_content(prompt)
            st.markdown(response.text)
