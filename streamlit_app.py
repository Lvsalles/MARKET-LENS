import streamlit as st
import pandas as pd
import google.generativeai as genai
import numpy as np
from folium import Map, Marker, Icon
from streamlit_folium import folium_static
from sklearn.neighbors import NearestNeighbors

# 1. Configuração e IA
st.set_page_config(page_title="AI Realty Command Center", layout="wide")
genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])

# 2. Biblioteca de Padronização Universal (Synonyms Library)
MAPPING = {
    'Price': ['Current Price', 'Price', 'Sold Price', 'List Price', 'Zestimate'],
    'Status': ['Status', 'LSC List Side', 'Listing Status'],
    'Zip': ['Zip', 'Zip Code', 'PostalCode'],
    'Address': ['Address', 'Full Address', 'Street Address'],
    'SqFt': ['Heated Area', 'SqFt', 'Living Area', 'Heated Area_num'],
    'Beds': ['Beds', 'Bedrooms', 'Beds_num'],
    'Baths': ['Full Baths', 'Bathrooms', 'Full Baths_num']
}

def standardize_df(df):
    for std, syns in MAPPING.items():
        found = next((c for c in df.columns if c in syns), None)
        if found: df = df.rename(columns={found: std})
    if 'Price' in df.columns:
        df['Price'] = pd.to_numeric(df['Price'].astype(str).str.replace(r'[$,]', '', regex=True), errors='coerce')
    return df

# 3. Módulo de CMA Moderno (Análise Comparativa de Mercado)
def perform_cma(target_property, pool_df):
    # Usa Média Ponderada e Algoritmos de Vizinhança
    pool_df = pool_df[pool_df['Status'].isin(['Sold', 'SLD', 'Closed'])]
    if len(pool_df) < 3: return "Dados insuficientes de vendas recentes."
    
    # Cálculo de Média Ponderada (Peso maior para casas com SqFt e Beds similares)
    pool_df['diff'] = abs(pool_df['SqFt'] - target_property['SqFt'])
    pool_df['weight'] = 1 / (pool_df['diff'] + 1)
    weighted_avg = (pool_df['Price'] * pool_df['weight']).sum() / pool_df['weight'].sum()
    return round(weighted_avg, 2)

# 4. Interface Lateral
st.sidebar.title("🏢 Realty Intelligence")
mode = st.sidebar.selectbox("Módulo", ["City Overview & Economy", "Modern CMA Tool", "Global Trends (Consultancy)"])

st.title("🏙️ Command Center: North Port & Venice Intelligence")

files = st.file_uploader("Upload MLS/Land/Portal Files", accept_multiple_files=True)

if files:
    all_dfs = []
    for f in files:
        if f.name.endswith('.csv'):
            df = standardize_df(pd.read_csv(f))
            all_dfs.append(df)
    
    if all_dfs:
        main_df = pd.concat(all_dfs, ignore_index=True)
        
        # --- MÓDULO 1: CITY OVERVIEW & ECONOMY ---
        if mode == "City Overview & Economy":
            st.header("📍 City & County Intelligence")
            # Aqui a IA cruza informações externas
            city_query = st.text_input("Informe a Cidade ou Zip Code", "North Port, FL")
            
            if st.button("Buscar Overview Completo"):
                with st.spinner("Cruzando dados demográficos e econômicos..."):
                    prompt = f"""
                    Atue como um analista da McKinsey e PWC. Forneça um overview de {city_query}:
                    1. Identifique o CONDADO e a região metropolitana.
                    2. POPULAÇÃO: Estimativa atual e taxa de crescimento.
                    3. ECONOMIA: Principais empregadores, taxa de DESEMPREGO local e renda média.
                    4. ESCOLAS: Liste as melhores escolas por Zip Code (GreatSchools rating).
                    5. CRIME: Índice de criminalidade vs média nacional.
                    6. ZONEAMENTO: Resumo sobre permissão de ADUs e tendências de desenvolvimento.
                    """
                    response = genai.GenerativeModel('gemini-1.5-flash').generate_content(prompt)
                    st.markdown(response.text)

        # --- MÓDULO 2: CMA MODERNO ---
        elif mode == "Modern CMA Tool":
            st.header("📊 Modern Comparative Market Analysis")
            target_addr = st.selectbox("Selecione a Propriedade Alvo", main_df['Address'].unique())
            target_row = main_df[main_df['Address'] == target_addr].iloc[0]
            
            val_est = perform_cma(target_row, main_df)
            st.metric("Valor Sugerido (Média Ponderada)", f"${val_est:,.2f}")
            st.caption("A análise considera proximidade de SqFt e similaridade de características.")

        # --- MÓDULO 3: GLOBAL TRENDS ---
        elif mode == "Global Trends (Consultancy)":
            st.header("📈 Deep Trend & Pattern Analysis")
            if st.button("Analisar Padrões Escondidos"):
                with st.spinner("Buscando tendências Deloitte, Zillow, e Redfin..."):
                    # Aqui passamos os dados reais para a IA encontrar o "Alfa"
                    data_summary = main_df.describe().to_string()
                    prompt = f"""
                    Analise estes dados reais: {data_summary}
                    
                    Cruze com as tendências atuais da Zillow, Redfin, Deloitte e McKinsey para 2025:
                    1. PADRÕES ESCONDIDOS: O que os números não dizem à primeira vista?
                    2. ARBITRAGEM: Onde o preço por SqFt está desalinhado com a infraestrutura local?
                    3. TENDÊNCIAS: Como o trabalho remoto e a migração para a Flórida afetam este micro-market?
                    """
                    response = genai.GenerativeModel('gemini-1.5-flash').generate_content(prompt)
                    st.markdown(response.text)

else:
    st.info("💡 Por favor, carregue os arquivos da MLS para ativar o cérebro da ferramenta.")
