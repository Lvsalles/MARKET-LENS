import streamlit as st
import pandas as pd
import google.generativeai as genai
import numpy as np
import folium
from streamlit_folium import folium_static
from pypdf import PdfReader
from docx import Document
import io

# 1. Configuração de Alta Performance
st.set_page_config(page_title="Realty Intelligence Hub", layout="wide")

# 2. Inicialização Segura da AI
if "GOOGLE_API_KEY" not in st.secrets:
    st.error("🔑 API Key em falta. Adicione-a nos Secrets do Streamlit.")
    st.stop()

genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])

# ---------------------------------------------------------
# BIBLIOTECA DE PADRONIZAÇÃO UNIVERSAL (SINÔNIMOS)
# ---------------------------------------------------------
SYNONYMS = {
    'Price': ['Current Price', 'Current Price_num', 'List Price', 'Sold Price', 'Price', 'Rent', 'Zestimate'],
    'Status': ['Status', 'Listing Status', 'LSC List Side', 'Status_clean'],
    'Zip': ['Zip', 'Zip Code', 'Zip_clean', 'PostalCode'],
    'Address': ['Address', 'Full Address', 'Street Address'],
    'SqFt': ['Heated Area', 'Heated Area_num', 'SqFt', 'Living Area'],
    'Beds': ['Beds', 'Bedrooms', 'Beds_num'],
    'Baths': ['Full Baths', 'Bathrooms', 'Full Baths_num'],
    'DOM': ['CDOM', 'ADOM', 'Days to Contract', 'DOM', 'CDOM_num'],
    'Zoning': ['Zoning', 'Zoning Code', 'Land Use'],
    'Agent': ['List Agent', 'Listing Agent', 'Agent Name', 'List Agent ID'],
    'Pool': ['Pool', 'Pool Features', 'Private Pool'],
    'Garage': ['Garage', 'Garage Spaces', 'Carport'],
    'Financing': ['Sold Terms', 'Terms', 'Financing']
}

STATUS_MAP = {
    'ACT': 'Active', 'SLD': 'Sold', 'PND': 'Pending', 'Closed': 'Sold', 'Active': 'Active'
}

def normalize_investor_data(df, filename):
    name = filename.lower()
    # Identificação de Categoria
    cat = "Residential"
    if "land" in name or "lots" in name or "total acreage_num" in df.columns: cat = "Land"
    elif "rent" in name or "rental" in name or "lease" in str(df.columns).lower(): cat = "Rental"
    
    # Padronização de Colunas
    for std, syns in SYNONYMS.items():
        found = next((c for c in syns if c in df.columns), None)
        if found: df = df.rename(columns={found: std})
    
    # Manter apenas colunas únicas
    df = df.loc[:, ~df.columns.duplicated(keep='last')]
    
    # Conversões Numéricas
    if 'Price' in df.columns:
        df['Price'] = pd.to_numeric(df['Price'].astype(str).str.replace(r'[$,]', '', regex=True), errors='coerce')
    if 'SqFt' in df.columns:
        df['Price_SqFt'] = df['Price'] / df['SqFt']
    if 'Status' in df.columns:
        df['Status'] = df['Status'].map(STATUS_MAP).fillna(df['Status'])
        
    return df, cat

# ---------------------------------------------------------
# INTERFACE SIDEBAR (CONTROLES DE INVESTIDOR)
# ---------------------------------------------------------
st.sidebar.title("💎 Terminal de Investimento")
analysis_mode = st.sidebar.selectbox(
    "Nível de Inteligência",
    ["Estratégia Macro & Econômica", "CMA Moderno (Média Ponderada)", "Ranking de Agentes & Performance", "Arbitragem Geográfica"]
)

report_target = st.sidebar.radio("Estilo de Relatório", ["Investidor Pro", "Apresentação para Cliente", "Due Diligence"])

# ---------------------------------------------------------
# UI PRINCIPAL
# ---------------------------------------------------------
st.title("🏙️ Real Estate Intelligence Command Center")
st.caption(f"Análise Ativa: {analysis_mode} | County Focus: Sarasota & Charlotte")
st.markdown("---")

uploaded_files = st.file_uploader("Upload MLS, Land, Rental or Research Files", accept_multiple_files=True)

if uploaded_files:
    all_dfs = []
    text_context = ""

    for f in uploaded_files:
        ext = f.name.split('.')[-1].lower()
        with st.expander(f"📦 Processando Variáveis: {f.name}"):
            try:
                if ext in ['csv', 'xlsx']:
                    raw = pd.read_csv(f) if ext == 'csv' else pd.read_excel(f)
                    df, category = normalize_investor_data(raw, f.name)
                    df['Category'] = category
                    all_dfs.append(df)
                    st.success(f"Identificado como {category}")
                    st.write(df.describe(include=[np.number]))
                elif ext == 'pdf':
                    text = " ".join([p.extract_text() for p in PdfReader(f).pages[:10]])
                    text_context += f"\n--- {f.name} ---\n{text}\n"
                elif ext == 'docx':
                    doc = Document(f)
                    text = "\n".join([p.text for p in doc.paragraphs])
                    text_context += f"\n--- {f.name} ---\n{text}\n"
            except Exception as e:
                st.error(f"Erro no ficheiro {f.name}: {e}")

    if all_dfs:
        main_df = pd.concat(all_dfs, ignore_index=True)
        
        # DASHBOARD DE MÉTRICAS (VISÃO GERAL)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Database Total", f"{len(main_df)} records")
        if 'Price' in main_df.columns:
            c2.metric("Preço Médio", f"${main_df['Price'].mean():,.0f}")
        if 'Price_SqFt' in main_df.columns:
            c3.metric("Avg $/SqFt", f"${main_df['Price_SqFt'].mean():,.2f}")
        c4.metric("Zip Codes", main_df['Zip'].nunique() if 'Zip' in main_df.columns else "N/A")

        # MAPEAMENTO GEO (ESTILO GOOGLE MAPS)
        st.subheader("📍 Geospatial Intelligence Heatmap")
        m = folium.Map(location=[27.05, -82.25], zoom_start=11)
        for _, row in main_df.dropna(subset=['Address']).head(50).iterrows():
            color = 'blue' if row['Category'] == 'Residential' else 'green'
            folium.Marker(
                [27.05 + np.random.uniform(-0.06, 0.06), -82.25 + np.random.uniform(-0.06, 0.06)],
                popup=f"Address: {row['Address']}<br
