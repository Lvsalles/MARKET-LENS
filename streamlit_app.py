import streamlit as st
import pandas as pd
import numpy as np
import google.generativeai as genai
import folium
from streamlit_folium import folium_static
from sklearn.linear_model import LinearRegression

# 1. SETUP DE ELITE
st.set_page_config(page_title="AI Strategic Investor Hub", layout="wide")

if "GOOGLE_API_KEY" in st.secrets:
    genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])
else:
    st.error("🔑 Configure a GOOGLE_API_KEY nos Secrets.")

# 2. BIBLIOTECA DE PADRONIZAÇÃO (Mapeia seus arquivos auditados)
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

def clean_and_normalize(df):
    """Resolve o InvalidIndexError e prepara os dados para análise."""
    # SOLUÇÃO DO ERRO: Remove colunas duplicadas (mantém a primeira ocorrência)
    df = df.loc[:, ~df.columns.duplicated()].copy()
    
    # Mapeamento de nomes
    for std, syns in SYNONYMS.items():
        found = next((c for c in syns if c in df.columns), None)
        if found:
            df = df.rename(columns={found: std})
    
    # Garantir colunas essenciais como NaN se não existirem
    essential = ['Price', 'SqFt', 'Beds', 'Baths', 'Zip', 'Address']
    for col in essential:
        if col not in df.columns:
            df[col] = np.nan
    
    # Limpeza numérica rigorosa
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
    df['SqFt'] = pd.to_numeric(df['SqFt'], errors='coerce')
    return df

# 3. UI PRINCIPAL
st.title("🏙️ Real Estate Strategic Command Center")
st.markdown("---")

files = st.file_uploader("Upload MLS Data (CSV/XLSX)", accept_multiple_files=True)

if files:
    all_dfs = []
    for f in files:
        try:
            df_raw = pd.read_csv(f) if f.name.endswith('.csv') else pd.read_excel(f)
            # Limpa cada arquivo individualmente antes de adicionar à lista
            all_dfs.append(clean_and_normalize(df_raw))
        except Exception as e:
            st.error(f"Erro ao ler {f.name}: {e}")

    if all_dfs:
        # CONCAT SEGURO: Agora garantimos que as colunas são únicas
        main_df = pd.concat(all_dfs, ignore_index=True)
        
        tab1, tab2, tab3 = st.tabs(["📍 Localização & Mapa", "📈 Arbitragem & ROI", "🧠 Estratégia AI"])

        with tab1:
            st.subheader("Mapa de Ativos (Google Maps Context)")
            # Foco em North Port/Venice
            m = folium.Map(location=[27.05, -82.25], zoom_start=11)
            
            plot_df = main_df.dropna(subset=['Address', 'Price']).head(100)
            for _, row in plot_df.iterrows():
                addr_url = str(row['Address']).replace(' ', '+')
                gmaps = f"https://www.google.com/maps/search/?api=1&query={addr_url}+FL"
                
                popup_html = f"<b>{row['Address']}</b><br>Price: ${row['Price']:,.0f}<br><a href='{gmaps}' target='_blank'>Abrir no Google Maps</a>"
                folium.Marker(
                    location=[27.05 + np.random.uniform(-0.04, 0.04), -82.25 + np.random.uniform(-0.04, 0.04)],
                    popup=folium.Popup(popup_html, max_width=300),
                    icon=folium.Icon(color='blue', icon='home')
                ).add_to(m)
            folium_static(m)

        with tab2:
            st.subheader("Análise Variável: Regressão de Arbitragem")
            # Modelo: Preço baseado em Tamanho e Quartos
            model_df = main_df.dropna(subset=['Price', 'SqFt', 'Beds']).copy()
            if len(model_df) > 5:
                X = model_df[['SqFt', 'Beds']].fillna(0)
                y = model_df['Price']
                reg = LinearRegression().fit(X, y)
                main_df['Fair_Value'] = reg.predict(main_df[['SqFt', 'Beds']].fillna(0))
                main_df['Arbitrage_%'] = ((main_df['Fair_Value'] - main_df['Price']) / main_df['Price']) * 100
                
                st.write("### 💎 Oportunidades Identificadas (Subvalorizados)")
                st.dataframe(main_df[main_df['Arbitrage_%'] > 10].sort_values(by='Arbitrage_%', ascending=False).head(10))
            else:
                st.warning("Dados insuficientes para rodar o modelo de regressão.")

        with tab3:
            if st.button("🚀 GERAR RELATÓRIO DE ELITE"):
                with st.spinner('AI analisando cruzamento de dados...'):
                    stats = main_df.describe().to_string()
                    prompt = f"""
                    Aja como um estrategista da McKinsey. Analise os dados de North Port/Venice: {stats}. 
                    Fale sobre o 'Sweet Spot' de investimento, zoneamento para ADU e tendências 2025.
                    """
                    model = genai.GenerativeModel('gemini-1.5-flash')
                    response = model.generate_content(prompt)
                    st.markdown(response.text)

else:
    st.info("💡 Carregue os arquivos para ativar a análise estratégica.")
