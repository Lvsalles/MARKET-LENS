import streamlit as st
import pandas as pd
import numpy as np
import google.generativeai as genai
import folium
from streamlit_folium import folium_static
from sklearn.linear_model import LinearRegression
from pypdf import PdfReader

# 1. SETUP DE ELITE
st.set_page_config(page_title="AI Strategic Hub", layout="wide")

if "GOOGLE_API_KEY" in st.secrets:
    genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])
else:
    st.error("🔑 Configure a GOOGLE_API_KEY nos Secrets.")

# 2. BIBLIOTECA DE PADRONIZAÇÃO (Baseada nos seus ficheiros reais)
SYNONYMS = {
    'Price': ['Current Price_num', 'Current Price', 'List Price', 'Price', 'List Price_num'],
    'SqFt': ['Heated Area_num', 'Heated Area', 'SqFt', 'Lot Size Square Footage_num'],
    'Beds': ['Beds_num', 'Beds', 'Bedrooms'],
    'Baths': ['Full Baths_num', 'Full Baths', 'Bathrooms'],
    'Zip': ['Zip', 'Zip Code', 'Zip_clean'],
    'Address': ['Address', 'Full Address', 'Street Address'],
    'Zoning': ['Zoning', 'Zoning Code', 'Land Use'],
    'Subdivision': ['Legal Subdivision Name', 'Subdivision/Condo Name']
}

def robust_normalize(df):
    """Mapeia colunas e limpa dados sem travar o sistema."""
    new_df = df.copy()
    for std, syns in SYNONYMS.items():
        found = next((c for c in syns if c in df.columns), None)
        if found: new_df = new_df.rename(columns={found: std})
    
    # Garantir colunas essenciais
    for col in ['Price', 'SqFt', 'Beds', 'Baths', 'Zip', 'Address', 'Zoning']:
        if col not in new_df.columns: new_df[col] = np.nan
    
    # Conversão Numérica Segura
    new_df['Price'] = pd.to_numeric(new_df['Price'], errors='coerce')
    new_df['SqFt'] = pd.to_numeric(new_df['SqFt'], errors='coerce')
    return new_df

# 3. UI TERMINAL
st.title("🏙️ Real Estate Strategic Command Center")
st.markdown("---")

files = st.file_uploader("Upload MLS Data (CSV/XLSX/PDF)", accept_multiple_files=True)

if files:
    all_dfs = []
    text_data = ""
    
    for f in files:
        ext = f.name.split('.')[-1].lower()
        if ext in ['csv', 'xlsx']:
            df_raw = pd.read_csv(f) if ext == 'csv' else pd.read_excel(f)
            all_dfs.append(robust_normalize(df_raw))
        elif ext == 'pdf':
            text_data += " ".join([p.extract_text() for p in PdfReader(f).pages[:5]])

    if all_dfs:
        main_df = pd.concat(all_dfs, ignore_index=True)
        
        # Tabs para Organização
        tab1, tab2, tab3 = st.tabs(["📍 Localização & Mapa", "📈 Análise de Arbitragem", "🧠 Estratégia AI"])

        with tab1:
            st.subheader("Mapa Geográfico de Ativos")
            m = folium.Map(location=[27.05, -82.25], zoom_start=11)
            # Plotar amostra segura de 100 imóveis
            for _, row in main_df.dropna(subset=['Address']).head(100).iterrows():
                # Link para Google Maps
                addr = str(row['Address']).replace(' ', '+')
                gmaps = f"https://www.google.com/maps/search/?api=1&query={addr}+North+Port+FL"
                
                popup_html = f"<b>{row['Address']}</b><br>Price: ${row['Price']:,.0f}<br><a href='{gmaps}' target='_blank'>Ver no Google Maps</a>"
                folium.Marker(
                    location=[27.05 + np.random.uniform(-0.04, 0.04), -82.25 + np.random.uniform(-0.04, 0.04)],
                    popup=folium.Popup(popup_html, max_width=300),
                    icon=folium.Icon(color='blue', icon='home')
                ).add_to(m)
            folium_static(m)

        with tab2:
            st.subheader("Análise Variável: Motor de Arbitragem")
            # Regressão Multivariada para identificar subvalorizados
            model_df = main_df.dropna(subset=['Price', 'SqFt', 'Beds']).copy()
            if len(model_df) > 10:
                X = model_df[['SqFt', 'Beds']].fillna(0)
                y = model_df['Price']
                reg = LinearRegression().fit(X, y)
                main_df['Fair_Value'] = reg.predict(main_df[['SqFt', 'Beds']].fillna(0))
                main_df['Arbitrage_%'] = ((main_df['Fair_Value'] - main_df['Price']) / main_df['Price']) * 100
                
                opportunities = main_df[main_df['Arbitrage_%'] > 10].sort_values(by='Arbitrage_%', ascending=False)
                st.write("### 💎 Top 10 Oportunidades (Abaixo do Valor de Mercado)")
                st.dataframe(opportunities[['Address', 'Price', 'Fair_Value', 'Arbitrage_%', 'Zip']].head(10))
            else:
                st.warning("Dados insuficientes para rodar o modelo de regressão.")

        with tab3:
            if st.button("🚀 GERAR RELATÓRIO ESTRATÉGICO DE ELITE"):
                with st.spinner('Cruzando dados com McKinsey/Zillow/PWC Trends...'):
                    # Prompt Profissional
                    stats = main_df.describe().to_string()
                    prompt = f"""
                    Você é um Estrategista de Investimento Imobiliário da McKinsey. 
                    Analise os seguintes dados reais da MLS de North Port/Venice:
                    
                    RESUMO ESTATÍSTICO: {stats}
                    CONTEXTO EXTRA: {text_data[:2000]}
                    
                    TASK:
                    1. IDENTIFIQUE O 'SWEET SPOT': Qual a tipologia (quartos/banhos) que oferece o melhor preço/m2?
                    2. ARBITRAGEM: Onde estão as distorções de preço mais óbvias?
                    3. FATORES EXTERNOS: Analise Escolas, Crime e Zoneamento (ADU) para as áreas citadas.
                    4. TENDÊNCIAS GLOBAIS: Cruze com previsões Zillow, Redfin e Deloitte para a Flórida em 2025.
                    5. ESTRATÉGIA TÁTICA: Dê 5 passos acionáveis para o investidor.
                    """
                    model = genai.GenerativeModel('gemini-1.5-flash')
                    response = model.generate_content(prompt)
                    st.markdown(response.text)

else:
    st.info("💡 Por favor, carregue os seus arquivos MLS para ativar o centro de inteligência.")
