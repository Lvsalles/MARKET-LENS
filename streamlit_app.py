import streamlit as st
import pandas as pd
import numpy as np
import google.generativeai as genai
from folium import Map, Marker, Icon, Circle, Popup
from streamlit_folium import folium_static
from sklearn.linear_model import LinearRegression
from pypdf import PdfReader
import io

# 1. SETUP DE ELITE
st.set_page_config(page_title="AI Strategic Command Center", layout="wide")

if "GOOGLE_API_KEY" not in st.secrets:
    st.error("🔑 API Key não encontrada nos Secrets.")
    st.stop()

genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])

# 2. BIBLIOTECA DE PADRONIZAÇÃO E ZONEAMENTO
SYNONYMS = {
    'Price': ['Current Price', 'Price', 'List Price', 'Sold Price', 'Zestimate'],
    'SqFt': ['Heated Area', 'Heated Area_num', 'SqFt', 'Living Area'],
    'Beds': ['Beds', 'Bedrooms', 'Beds_num'],
    'Baths': ['Full Baths', 'Bathrooms', 'Full Baths_num'],
    'Zip': ['Zip', 'Zip Code', 'PostalCode'],
    'Status': ['Status', 'LSC List Side', 'Listing Status'],
    'Address': ['Address', 'Full Address', 'Street Address'],
    'Zoning': ['Zoning', 'Zoning Code', 'Land Use'],
    'Year': ['Year Built', 'Year Built_num'],
    'Pool': ['Pool', 'Pool Private', 'Pool Features']
}

# Regras de ADU baseadas em zoneamentos comuns de Sarasota/Charlotte
ADU_VIABILITY = {
    'RSF1': 'Low - Strict Setbacks',
    'RSF2': 'High - Accessory Unit Friendly',
    'RSF3': 'Moderate - Zoning Review Required',
    'RMH': 'High - Specific for Mobile/Manufactured',
    'AG': 'Maximum - Large Acreage Potential'
}

# 3. MOTORES DE PROCESSAMENTO
def normalize_investor_data(df):
    for std, syns in SYNONYMS.items():
        found = next((c for c in syns if c in df.columns), None)
        if found: df = df.rename(columns={found: std})
    
    # Limpeza de dados
    if 'Price' in df.columns:
        df['Price'] = pd.to_numeric(df['Price'].astype(str).str.replace(r'[$,]', '', regex=True), errors='coerce')
    if 'SqFt' in df.columns:
        df['SqFt'] = pd.to_numeric(df['SqFt'], errors='coerce')
    
    return df.dropna(subset=['Price', 'SqFt'])

def perform_arbitrage_regression(df):
    """Calcula o 'Fair Value' usando Regressão Multivariada"""
    if len(df) < 5: return df
    X = df[['SqFt', 'Beds', 'Baths']].fillna(0)
    y = df['Price']
    model = LinearRegression().fit(X, y)
    df['Fair_Value'] = model.predict(X)
    df['Arbitrage_Gap'] = df['Fair_Value'] - df['Price']
    df['Opportunity_Score'] = (df['Arbitrage_Gap'] / df['Price']) * 100
    return df

# 4. INTERFACE SIDEBAR
st.sidebar.title("💎 Intelligence Terminal")
module = st.sidebar.selectbox("Módulo de Análise", 
    ["Overview Econômico & Social", "CMA & Arbitragem Preditiva", "Viabilidade de ADU & Zoneamento"])

report_style = st.sidebar.radio("Estilo de Consultoria", ["McKinsey & Co", "Deloitte Strategy", "Zillow/Redfin Trendline"])

# 5. UI PRINCIPAL
st.title("🏙️ Real Estate Intelligence Command Center")
st.markdown("---")

files = st.file_uploader("Upload MLS Data, Zoning PDFs or Market Reports", accept_multiple_files=True)

if files:
    dfs = []
    docs_text = ""
    
    for f in files:
        ext = f.name.split('.')[-1].lower()
        if ext in ['csv', 'xlsx']:
            df = pd.read_csv(f) if ext == 'csv' else pd.read_excel(f)
            dfs.append(normalize_investor_data(df))
        elif ext == 'pdf':
            docs_text += " ".join([p.extract_text() for p in PdfReader(f).pages[:5]])

    if dfs:
        master_df = pd.concat(dfs, ignore_index=True)
        
        # Módulo de Mapeamento Geoespacial
        st.subheader("📍 Localização Estratégica & Densidade")
        m = Map(location=[27.05, -82.25], zoom_start=11)
        for _, row in master_df.dropna(subset=['Address']).head(100).iterrows():
            # Coordenadas aproximadas por dispersão se não houver Lat/Long
            folium_loc = [27.05 + np.random.uniform(-0.06, 0.06), -82.25 + np.random.uniform(-0.06, 0.06)]
            color = 'green' if row.get('Arbitrage_Gap', 0) > 20000 else 'blue'
            Marker(location=folium_loc, 
                   popup=f"{row['Address']}<br>Price: ${row['Price']:,.0f}",
                   icon=Icon(color=color, icon='home')).add_to(m)
        folium_static(m)

        # Módulo Analítico por Variável
        if module == "CMA & Arbitragem Preditiva":
            st.subheader("📊 Arbitragem Baseada em Regressão Multivariada")
            scored_df = perform_arbitrage_regression(master_df)
            st.dataframe(scored_df[['Address', 'Price', 'Fair_Value', 'Opportunity_Score', 'Beds', 'SqFt']].sort_values(by='Opportunity_Score', ascending=False).head(10))
            st.info("💡 Propriedades com Opportunity Score positivo estão SUBVALORIZADAS em relação ao mercado local.")

        # BOTÃO DE GERAR RELATÓRIO
        st.markdown("---")
        if st.button("🚀 GERAR RELATÓRIO ESTRATÉGICO INTEGRADO"):
            with st.spinner('AI analisando cruzamentos socioeconômicos e tendências globais...'):
                try:
                    # Payload de dados para a IA
                    context = {
                        "stats": master_df.describe().to_string(),
                        "zips": master_df['Zip'].value_counts().head(5).to_dict() if 'Zip' in master_df.columns else "N/A",
                        "adu": master_df['Zoning'].value_counts().to_dict() if 'Zoning' in master_df.columns else "N/A"
                    }

                    prompt = f"""
                    Aja como um Senior Strategist da {report_style}. Analise estes dados reais:
                    
                    DADOS MLS: {context['stats']}
                    ZIP HOTSPOTS: {context['zips']}
                    ZONEAMENTO: {context['adu']}
                    TEXTO EXTRA: {docs_text[:2000]}

                    OBJETIVOS DO RELATÓRIO:
                    1. OVERVIEW DA CIDADE: Identifique Condado, População, Emprego e Escolas por Zip Code.
                    2. ARBITRAGEM: Identifique padrões escondidos onde quartos/piscina não estão precificados corretamente.
                    3. ZONEAMENTO E ADU: Aponte quais propriedades têm maior potencial de lucro via construção adicional.
                    4. TENDÊNCIAS: Cruze com dados atuais de Zillow, Redfin e consultorias globais para o mercado de 2025.
                    5. PRÓXIMOS PASSOS NO CÓDIGO: Enriquecimento de dados e Modelagem Preditiva.
                    
                    Linguagem: Profissional English.
                    """
                    
                    response = genai.GenerativeModel('gemini-1.5-flash').generate_content(prompt)
                    st.markdown("### 📊 Strategic Intelligence & Due Diligence")
                    st.write(response.text)
                    st.balloons()
                except Exception as e:
                    st.error(f"Erro na IA: {e}")
