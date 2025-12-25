import streamlit as st
import pandas as pd
import numpy as np
import google.generativeai as genai
import folium
from streamlit_folium import folium_static
from sklearn.linear_model import LinearRegression

# 1. SETUP DE INTELIGÊNCIA
st.set_page_config(page_title="Market Lens Pro", layout="wide")

if "GOOGLE_API_KEY" in st.secrets:
    genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])
    model = genai.GenerativeModel('gemini-1.5-flash')
else:
    st.error("🔑 API Key não configurada nos Secrets!")
    st.stop()

# 2. BIBLIOTECA DE PADRONIZAÇÃO (Dicionário de Sinônimos Robusto)
# Esta biblioteca mapeia os nomes técnicos do MLS para nomes funcionais.
SYNONYMS = {
    'Price': ['Current Price_num', 'Current Price', 'Price', 'List Price', 'Price_clean'],
    'SqFt': ['Heated Area_num', 'SqFt', 'Living Area', 'Heated Area', 'Lot Size Square Footage_num'],
    'Beds': ['Beds_num', 'Beds', 'Bedrooms', 'Bed'],
    'Baths': ['Full Baths_num', 'Full Baths', 'Bathrooms', 'Baths'],
    'Address': ['Address', 'Full Address', 'Street Address', 'Property Address'],
    'Zip': ['Zip', 'Zip Code', 'PostalCode', 'Zip_clean'],
    'Status': ['Status', 'Listing Status', 'Status_clean'],
    'Zoning': ['Zoning', 'Zoning Code', 'Land Use']
}

def robust_normalize(df):
    """Resolve o InvalidIndexError e normaliza as colunas de cada arquivo."""
    # A) REMOVE COLUNAS DUPLICADAS: O coração do erro de índice.
    # Se o arquivo tem duas colunas "Status", mantemos apenas a primeira.
    df = df.loc[:, ~df.columns.duplicated()].copy()
    
    # B) MAPEAMENTO POR SINÔNIMOS:
    for target_name, synonyms_list in SYNONYMS.items():
        found = next((c for c in synonyms_list if c in df.columns), None)
        if found:
            df = df.rename(columns={found: target_name})
    
    # C) GARANTIA DE COLUNAS: Cria a coluna como vazia se não existir.
    essential_cols = ['Price', 'SqFt', 'Beds', 'Address', 'Zip']
    for col in essential_cols:
        if col not in df.columns:
            df[col] = np.nan
            
    # D) LIMPEZA NUMÉRICA: Converte strings de preço ($250,000) em números reais.
    if 'Price' in df.columns:
        df['Price'] = pd.to_numeric(df['Price'].astype(str).str.replace(r'[$,]', '', regex=True), errors='coerce')
    if 'SqFt' in df.columns:
        df['SqFt'] = pd.to_numeric(df['SqFt'], errors='coerce')
        
    return df

# 3. INTERFACE DA WEBTOOL
st.title("🏙️ Real Estate Strategic Engine")
st.markdown("---")

files = st.file_uploader("Upload MLS Files (CSV/XLSX)", accept_multiple_files=True)

if files:
    all_dfs = []
    for f in files:
        try:
            # Carregamento inicial do arquivo bruto
            df_raw = pd.read_csv(f) if f.name.endswith('.csv') else pd.read_excel(f)
            
            # NORMALIZAÇÃO IMEDIATA (Antes de adicionar à lista de junção)
            df_cleaned = robust_normalize(df_raw)
            all_dfs.append(df_cleaned)
            st.sidebar.success(f"✅ {f.name} processado.")
        except Exception as e:
            st.error(f"Erro ao processar {f.name}: {e}")

    if all_dfs:
        # CONCATENAÇÃO SEGURA: Agora garantimos que as colunas são únicas e nomeadas igual.
        # O ignore_index=True reconstrói o índice do zero, evitando o erro.
        main_df = pd.concat(all_dfs, ignore_index=True)
        
        tab_map, tab_arbitrage, tab_gemini = st.tabs(["📍 Mapa de Ativos", "📈 Arbitragem & ROI", "🤖 IA Strategic Chat"])

        with tab_map:
            st.subheader("Geolocalização e Contexto de Vizinhança")
            m = folium.Map(location=[27.05, -82.25], zoom_start=11)
            
            # Plotagem inteligente: apenas se tiver endereço e preço
            plot_df = main_df.dropna(subset=['Address', 'Price']).head(150)
            for _, row in plot_df.iterrows():
                addr_encoded = str(row['Address']).replace(' ', '+')
                gmaps_link = f"https://www.google.com/maps/search/?api=1&query={addr_encoded}+FL"
                
                popup_html = f"""
                <div style='width:220px'>
                    <b>{row['Address']}</b><br>
                    Preço: ${row['Price']:,.0f}<br>
                    SqFt: {row['SqFt']}<br>
                    <a href='{gmaps_link}' target='_blank'>🔗 Ver no Google Maps</a>
                </div>
                """
                folium.Marker(
                    location=[27.05 + np.random.uniform(-0.04, 0.04), -82.25 + np.random.uniform(-0.04, 0.04)],
                    popup=folium.Popup(popup_html, max_width=300),
                    icon=folium.Icon(color='blue', icon='home')
                ).add_to(m)
            folium_static(m)

        with tab_arbitrage:
            st.subheader("Modelo de Arbitragem (CMA Automatizado)")
            # Motor de Regressão: Preço baseado em SqFt e Quartos
            model_df = main_df.dropna(subset=['Price', 'SqFt', 'Beds']).copy()
            if len(model_df) > 5:
                X = model_df[['SqFt', 'Beds']].fillna(0)
                y = model_df['Price']
                reg = LinearRegression().fit(X, y)
                main_df['Fair_Value'] = reg.predict(main_df[['SqFt', 'Beds']].fillna(0))
                main_df['Arbitrage_%'] = ((main_df['Fair_Value'] - main_df['Price']) / main_df['Price']) * 100
                
                st.write("### 💎 Top 15 Oportunidades Subvalorizadas")
                st.dataframe(main_df[main_df['Arbitrage_%'] > 5].sort_values(by='Arbitrage_%', ascending=False).head(15))
            else:
                st.warning("Carregue mais dados (Residencial/Land) para ativar o motor de regressão.")

        with tab_gemini:
            st.subheader("Consultoria Estratégica AI (McKinsey Style)")
            user_input = st.text_input("Pergunte sobre tendências, zoneamento ou ROI:")
            if user_input:
                with st.spinner('A IA está processando o contexto do mercado...'):
                    # Conexão do motor de dados com o Gemini
                    stats_summary = main_df.describe().to_string()
                    prompt = f"""
                    Aja como um analista sênior de investimentos imobiliários.
                    Dados atuais do mercado (North Port/Venice): {stats_summary}
                    
                    Pergunta do Usuário: {user_input}
                    
                    Sua resposta deve considerar:
                    1. Oportunidades de Arbitragem baseadas no preço por SqFt.
                    2. Tendências de 2025 (Zillow/Redfin).
                    3. Potencial de valorização e zoneamento (ADUs).
                    """
                    response = model.generate_content(prompt)
                    st.markdown(response.text)

else:
    st.info("💡 Hub Ativo. Arraste os arquivos auditados para iniciar.")
