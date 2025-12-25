import streamlit as st
import pandas as pd
import numpy as np
import google.generativeai as genai
import folium
from streamlit_folium import folium_static
from sklearn.linear_model import LinearRegression

# 1. Setup de Inteligência
st.set_page_config(page_title="AI Investor Command Center", layout="wide")
if "GOOGLE_API_KEY" in st.secrets:
    genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])

# 2. Biblioteca de Padronização (Crucial para os seus arquivos auditados)
SYNONYMS = {
    'Price': ['Current Price_num', 'Current Price', 'Price', 'List Price'],
    'SqFt': ['Heated Area_num', 'Heated Area', 'SqFt', 'Lot Size Square Footage_num'],
    'Beds': ['Beds_num', 'Beds', 'Bedrooms'],
    'Baths': ['Full Baths_num', 'Full Baths', 'Bathrooms'],
    'Zip': ['Zip', 'Zip Code'],
    'Address': ['Address', 'Full Address', 'Street Address'],
    'Zoning': ['Zoning', 'Zoning Code']
}

def clean_and_normalize(df):
    """Remove colunas duplicadas e padroniza nomes."""
    # RESOLUÇÃO DO ERRO: Remove colunas duplicadas (ex: se houver duas colunas 'Status')
    df = df.loc[:, ~df.columns.duplicated()].copy()
    
    # Mapeamento de nomes
    for std, syns in SYNONYMS.items():
        found = next((c for c in syns if c in df.columns), None)
        if found:
            df = df.rename(columns={found: std})
            
    # Garantir colunas mínimas para não quebrar o código
    for col in ['Price', 'SqFt', 'Beds', 'Address', 'Zip']:
        if col not in df.columns:
            df[col] = np.nan
            
    # Conversão numérica
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
    df['SqFt'] = pd.to_numeric(df['SqFt'], errors='coerce')
    return df

# 3. Interface Principal
st.title("🏙️ Global Real Estate Strategic Station")

files = st.file_uploader("Upload MLS Data (CSV/XLSX)", accept_multiple_files=True)

if files:
    all_dfs = []
    for f in files:
        try:
            df_raw = pd.read_csv(f) if f.name.endswith('.csv') else pd.read_excel(f)
            # Normaliza cada arquivo individualmente antes de juntar
            all_dfs.append(clean_and_normalize(df_raw))
        except Exception as e:
            st.error(f"Erro ao ler {f.name}: {e}")

    if all_dfs:
        # Agora o concat funcionará sem o erro de Index
        main_df = pd.concat(all_dfs, ignore_index=True)
        
        tab1, tab2, tab3 = st.tabs(["📍 Localização & Mapas", "📈 Análise de Arbitragem", "🧠 AI Report"])

        with tab1:
            st.subheader("Mapa de Ativos (Google Maps Context)")
            # Centralizado em North Port/Venice
            m = folium.Map(location=[27.05, -82.25], zoom_start=11)
            
            # Plotagem inteligente por endereço
            plot_df = main_df.dropna(subset=['Address', 'Price']).head(100)
            for _, row in plot_df.iterrows():
                addr = str(row['Address']).replace(' ', '+')
                gmaps_url = f"https://www.google.com/maps/search/?api=1&query={addr}+FL"
                
                popup_text = f"<b>{row['Address']}</b><br>Price: ${row['Price']:,.0f}<br><a href='{gmaps_url}' target='_blank'>Street View</a>"
                folium.Marker(
                    location=[27.05 + np.random.uniform(-0.03, 0.03), -82.25 + np.random.uniform(-0.03, 0.03)],
                    popup=folium.Popup(popup_text, max_width=300),
                    icon=folium.Icon(color='blue', icon='home')
                ).add_to(m)
            folium_static(m)

        with tab2:
            st.subheader("Análise Variável: Regressão de Preço")
            # Modelo preditivo simples (SqFt + Beds -> Price)
            model_df = main_df.dropna(subset=['Price', 'SqFt', 'Beds']).copy()
            if len(model_df) > 5:
                reg = LinearRegression().fit(model_df[['SqFt', 'Beds']], model_df['Price'])
                main_df['Fair_Value'] = reg.predict(main_df[['SqFt', 'Beds']].fillna(0))
                main_df['Arbitrage_%'] = ((main_df['Fair_Value'] - main_df['Price']) / main_df['Price']) * 100
                
                st.write("### 💎 Imóveis Subavaliados (Oportunidades)")
                st.dataframe(main_df[main_df['Arbitrage_%'] > 10].sort_values(by='Arbitrage_%', ascending=False))

        with tab3:
            if st.button("🚀 Gerar Relatório McKinsey"):
                with st.spinner('AI analisando cruzamento de dados...'):
                    stats = main_df.describe().to_string()
                    prompt = f"Aja como um estrategista. Analise os dados de North Port/Venice: {stats}. Fale sobre tendências 2025, escolas e zoneamento ADU."
                    model = genai.GenerativeModel('gemini-1.5-flash')
                    response = model.generate_content(prompt)
                    st.markdown(response.text)
