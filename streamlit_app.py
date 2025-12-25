import streamlit as st
import pandas as pd
import numpy as np
import google.generativeai as genai
import folium
from streamlit_folium import folium_static
from sklearn.linear_model import LinearRegression
from pypdf import PdfReader

# 1. Configuração de Elite
st.set_page_config(page_title="AI Strategic Hub", layout="wide")

if "GOOGLE_API_KEY" in st.secrets:
    genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])
else:
    st.error("🔑 Configure a GOOGLE_API_KEY nos Secrets.")

# 2. Biblioteca de Padronização Inteligente (Mapeia seus arquivos auditados)
SYNONYMS = {
    'Price': ['Current Price_num', 'Current Price', 'List Price', 'Price'],
    'SqFt': ['Heated Area_num', 'Heated Area', 'SqFt', 'Lot Size Square Footage_num'],
    'Beds': ['Beds_num', 'Beds', 'Bedrooms'],
    'Baths': ['Full Baths_num', 'Full Baths', 'Bathrooms'],
    'Zip': ['Zip', 'Zip Code', 'Zip_clean'],
    'Address': ['Address', 'Full Address', 'Street Address'],
    'Status': ['Status', 'Status_clean'],
    'Zoning': ['Zoning', 'Zoning Code']
}

def robust_normalize(df):
    """Mapeia colunas e garante que o código não quebre se faltarem dados."""
    new_df = df.copy()
    for std, syns in SYNONYMS.items():
        found = next((c for c in syns if c in df.columns), None)
        if found:
            new_df = new_df.rename(columns={found: std})
    
    # Criar colunas como NaN se não existirem (evita KeyError)
    for col in ['Price', 'SqFt', 'Beds', 'Baths', 'Zip', 'Address', 'Status']:
        if col not in new_df.columns:
            new_df[col] = np.nan
    
    # Limpeza numérica
    new_df['Price'] = pd.to_numeric(new_df['Price'], errors='coerce')
    new_df['SqFt'] = pd.to_numeric(new_df['SqFt'], errors='coerce')
    return new_df

# 3. Interface Principal
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
        
        tab1, tab2, tab3 = st.tabs(["📍 Mapa & Localização", "📈 Arbitragem & Variáveis", "🧠 Estratégia AI"])

        with tab1:
            st.subheader("Mapa Geográfico de Ativos")
            # Centraliza em North Port
            m = folium.Map(location=[27.05, -82.25], zoom_start=11)
            
            # Plotagem segura: apenas se tiver endereço e preço
            plot_df = main_df.dropna(subset=['Address', 'Price']).head(100)
            for _, row in plot_df.iterrows():
                addr_encoded = str(row['Address']).replace(' ', '+')
                gmaps_url = f"https://www.google.com/maps/search/?api=1&query={addr_encoded}+FL"
                
                popup_html = f"""
                <b>{row['Address']}</b><br>
                Preço: ${row['Price']:,.0f}<br>
                <a href="{gmaps_url}" target="_blank">Ver no Google Maps</a>
                """
                folium.Marker(
                    location=[27.05 + np.random.uniform(-0.04, 0.04), -82.25 + np.random.uniform(-0.04, 0.04)],
                    popup=folium.Popup(popup_html, max_width=300),
                    icon=folium.Icon(color='blue', icon='home')
                ).add_to(m)
            folium_static(m)

        with tab2:
            st.subheader("Análise Variável por Variável")
            # Motor de Regressão para encontrar subvalorizados
            model_df = main_df.dropna(subset=['Price', 'SqFt', 'Beds']).copy()
            if len(model_df) > 5:
                X = model_df[['SqFt', 'Beds']].fillna(0)
                y = model_df['Price']
                reg = LinearRegression().fit(X, y)
                main_df['Fair_Value'] = reg.predict(main_df[['SqFt', 'Beds']].fillna(0))
                main_df['Opportunity_%'] = ((main_df['Fair_Value'] - main_df['Price']) / main_df['Price']) * 100
                
                st.write("### 💎 Top Oportunidades de Arbitragem")
                st.dataframe(main_df[main_df['Opportunity_%'] > 5].sort_values(by='Opportunity_%', ascending=False).head(10))
            else:
                st.warning("Carregue mais dados residenciais para ativar a regressão.")

        with tab3:
            if st.button("🚀 GERAR RELATÓRIO ESTRATÉGICO"):
                with st.spinner('AI analisando o mercado...'):
                    stats = main_df.describe().to_string()
                    prompt = f"""
                    Aja como um Estrategista da McKinsey. Analise os dados de North Port/Venice:
                    DADOS: {stats}
                    OBJETIVO: Identificar Sweet Spots (Preço/SqFt), tendências de Escolas/Crime 
                    e potencial de ADU (Zoneamento). Cite tendências Zillow/Redfin 2025.
                    """
                    model = genai.GenerativeModel('gemini-1.5-flash')
                    response = model.generate_content(prompt)
                    st.markdown(response.text)

else:
    st.info("💡 Aguardando arquivos para iniciar a central de inteligência.")
