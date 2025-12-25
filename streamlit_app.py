import streamlit as st
import pandas as pd
import numpy as np
import google.generativeai as genai
import folium
from streamlit_folium import folium_static
from sklearn.linear_model import LinearRegression

# 1. SETUP DE INTELIGÊNCIA (Gemini 1.5 Flash)
st.set_page_config(page_title="Market Lens Intelligence", layout="wide")

if "GOOGLE_API_KEY" in st.secrets:
    genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])
    model = genai.GenerativeModel('gemini-1.5-flash')
else:
    st.error("🔑 API Key não configurada nos Secrets!")
    st.stop()

# 2. BIBLIOTECA DE PADRONIZAÇÃO (Dicionário de Sinónimos)
SYNONYMS = {
    'Price': ['Current Price_num', 'Current Price', 'Price', 'List Price'],
    'SqFt': ['Heated Area_num', 'SqFt', 'Living Area', 'Lot Size Square Footage_num'],
    'Beds': ['Beds_num', 'Beds', 'Bedrooms'],
    'Baths': ['Full Baths_num', 'Full Baths', 'Bathrooms'],
    'Address': ['Address', 'Full Address', 'Street Address'],
    'Zip': ['Zip', 'Zip Code', 'PostalCode'],
    'Zoning': ['Zoning', 'Zoning Code', 'Land Use']
}

def robust_cleaning(df):
    """Extermina o InvalidIndexError e normaliza os dados."""
    # LIMPEZA CRÍTICA: Remove colunas duplicadas (mantém apenas a primeira)
    df = df.loc[:, ~df.columns.duplicated()].copy()
    
    # Padronização de nomes
    for target, syns in SYNONYMS.items():
        found = next((c for c in syns if c in df.columns), None)
        if found:
            df = df.rename(columns={found: target})
    
    # Garantia de variáveis essenciais (evita erros de cálculo)
    for col in ['Price', 'SqFt', 'Beds', 'Address', 'Zip']:
        if col not in df.columns:
            df[col] = np.nan
            
    # Limpeza de preços e conversão numérica
    if 'Price' in df.columns:
        df['Price'] = pd.to_numeric(df['Price'].astype(str).str.replace(r'[$,]', '', regex=True), errors='coerce')
    if 'SqFt' in df.columns:
        df['SqFt'] = pd.to_numeric(df['SqFt'], errors='coerce')
        
    return df

# 3. INTERFACE DA WEBTOOL
st.title("🏙️ Global Real Estate Strategic Engine")
st.markdown("---")

files = st.file_uploader("Upload MLS Files (CSV/XLSX)", accept_multiple_files=True)

if files:
    all_dfs = []
    for f in files:
        try:
            df_raw = pd.read_csv(f) if f.name.endswith('.csv') else pd.read_excel(f)
            # Limpa cada ficheiro ANTES do concat para evitar InvalidIndexError
            all_dfs.append(robust_cleaning(df_raw))
            st.sidebar.success(f"✅ {f.name} processado")
        except Exception as e:
            st.error(f"Erro no ficheiro {f.name}: {e}")

    if all_dfs:
        # CONCATENAÇÃO SEGURA
        main_df = pd.concat(all_dfs, ignore_index=True)
        
        tab_map, tab_analytics, tab_gemini = st.tabs(["📍 Mapa de Ativos", "📈 Arbitragem & ROI", "🤖 Consultoria Gemini"])

        with tab_map:
            st.subheader("Geolocalização e Contexto de Vizinhança")
            m = folium.Map(location=[27.05, -82.25], zoom_start=11)
            
            # Plotagem inteligente com link para Google Maps
            plot_df = main_df.dropna(subset=['Address', 'Price']).head(150)
            for _, row in plot_df.iterrows():
                addr_url = str(row['Address']).replace(' ', '+')
                gmaps_link = f"https://www.google.com/maps/search/?api=1&query={addr_url}+FL"
                
                popup_html = f"""
                <div style='width:220px'>
                    <b>{row['Address']}</b><br>
                    Preço: ${row['Price']:,.0f}<br>
                    <a href='{gmaps_link}' target='_blank'>🔗 Abrir no Google Maps</a>
                </div>
                """
                folium.Marker(
                    location=[27.05 + np.random.uniform(-0.04, 0.04), -82.25 + np.random.uniform(-0.04, 0.04)],
                    popup=folium.Popup(popup_html, max_width=300),
                    icon=folium.Icon(color='blue', icon='home')
                ).add_to(m)
            folium_static(m)

        with tab_analytics:
            st.subheader("Análise Preditiva de Arbitragem")
            # Motor de Regressão: Preço baseado em SqFt e Quartos
            model_df = main_df.dropna(subset=['Price', 'SqFt', 'Beds']).copy()
            if len(model_df) > 5:
                X = model_df[['SqFt', 'Beds']].fillna(0)
                y = model_df['Price']
                reg = LinearRegression().fit(X, y)
                main_df['Fair_Value'] = reg.predict(main_df[['SqFt', 'Beds']].fillna(0))
                main_df['Arbitrage_%'] = ((main_df['Fair_Value'] - main_df['Price']) / main_df['Price']) * 100
                
                st.write("### 💎 Top Oportunidades Subvalorizadas")
                st.dataframe(main_df[main_df['Arbitrage_%'] > 5].sort_values(by='Arbitrage_%', ascending=False).head(15))
            else:
                st.warning("Dados insuficientes para análise estatística.")

        with tab_gemini:
            st.subheader("🤖 Assistente de Investimento (McKinsey Style)")
            user_query = st.text_input("Ex: Qual o potencial de construir uma Guest House (ADU) nestes terrenos?")
            
            if user_query:
                with st.spinner('A analisar o mercado...'):
                    # Conectando o motor ao cérebro (Gemini)
                    summary = main_df.describe().to_string()
                    prompt = f"""
                    Dados Reais do Mercado: {summary}
                    Pergunta do Investidor: {user_query}
                    
                    Aja como um analista sénior. Considere zoneamento, preço por SqFt e tendências 2025.
                    """
                    response = model.generate_content(prompt)
                    st.markdown(response.text)

else:
    st.info("💡 Aguardando ficheiros para ativar o Command Center.")
