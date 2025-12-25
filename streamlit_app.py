import streamlit as st
import pandas as pd
import google.generativeai as genai
import numpy as np
import folium
from streamlit_folium import folium_static
from pypdf import PdfReader
from docx import Document
from sklearn.neighbors import NearestNeighbors

# 1. Configuração de Alta Performance
st.set_page_config(page_title="AI Investor Command Center", layout="wide", initial_sidebar_state="expanded")

# 2. Inicialização da AI
if "GOOGLE_API_KEY" not in st.secrets:
    st.error("🔑 API Key em falta nos Secrets do Streamlit.")
    st.stop()

genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])

# ---------------------------------------------------------
# BIBLIOTECA DE PADRONIZAÇÃO UNIVERSAL (REALTOR & INVESTOR)
# ---------------------------------------------------------
SYNONYMS = {
    'Price': ['Current Price', 'Current Price_num', 'Sold Price', 'List Price', 'Zestimate', 'Price'],
    'Status': ['Status', 'Listing Status', 'LSC List Side', 'Status_clean'],
    'Zip': ['Zip', 'Zip Code', 'Zip_clean', 'PostalCode'],
    'Address': ['Address', 'Full Address', 'Street Address'],
    'SqFt': ['Heated Area', 'Heated Area_num', 'SqFt', 'Living Area'],
    'Beds': ['Beds', 'Beds_num', 'Bedrooms'],
    'Baths': ['Full Baths', 'Full Baths_num', 'Bathrooms'],
    'DOM': ['CDOM', 'ADOM', 'Days to Contract', 'DOM', 'CDOM_num'],
    'Zoning': ['Zoning', 'Zoning Code', 'Land Use'],
    'Agent': ['List Agent', 'Listing Agent', 'Agent Name']
}

def normalize_investor_data(df):
    for std, syns in SYNONYMS.items():
        found = next((c for c in df.columns if c in syns), None)
        if found: df = df.rename(columns={found: std})
    
    # Limpeza de duplicados e tipos
    df = df.loc[:, ~df.columns.duplicated(keep='last')]
    if 'Price' in df.columns:
        df['Price'] = pd.to_numeric(df['Price'].astype(str).str.replace(r'[$,]', '', regex=True), errors='coerce')
    if 'SqFt' in df.columns:
        df['Price_SqFt'] = df['Price'] / df['SqFt']
    return df

# ---------------------------------------------------------
# INTERFACE LATERAL (PAINEL DE CONTROLO)
# ---------------------------------------------------------
st.sidebar.title("💎 Investor Hub")
analysis_mode = st.sidebar.selectbox(
    "Nível de Análise",
    ["Estratégia Macro (Cidade/Economia)", "CMA Moderno (Avaliação)", "Arbitragem e Zonas Oportunas", "Auditoria de Agentes & Portais"]
)

report_depth = st.sidebar.radio("Profundidade do Relatório", ["Executivo", "Técnico Detalhado", "Análise de Risco (Due Diligence)"])

# ---------------------------------------------------------
# MOTOR PRINCIPAL
# ---------------------------------------------------------
st.title("🏙️ Ultimate Real Estate Intelligence Hub")
st.caption(f"Análise Ativa: {analysis_mode} | Fonte: MLS & Global Consultancies")
st.markdown("---")

uploaded_files = st.file_uploader("Suba os seus ficheiros (MLS, Land, Rentals, Zillow, Docs)", accept_multiple_files=True)

if uploaded_files:
    master_context = ""
    dfs = []

    for f in uploaded_files:
        ext = f.name.split('.')[-1].lower()
        with st.expander(f"📁 Processando: {f.name}"):
            try:
                if ext in ['csv', 'xlsx']:
                    raw = pd.read_csv(f) if ext == 'csv' else pd.read_excel(f)
                    df = normalize_investor_data(raw)
                    dfs.append(df)
                    st.success("Dados normalizados com sucesso.")
                elif ext == 'pdf':
                    text = " ".join([p.extract_text() for p in PdfReader(f).pages[:5]])
                    master_context += f"\n[DOC: {f.name}]\n{text[:2000]}\n"
            except Exception as e:
                st.error(f"Erro: {e}")

    if dfs:
        main_df = pd.concat(dfs, ignore_index=True)
        
        # Métrica em Tempo Real
        m1, m2, m3 = st.columns(3)
        m1.metric("Preço Médio", f"${main_df['Price'].mean():,.0f}")
        m2.metric("Média $/SqFt", f"${main_df.get('Price_SqFt', pd.Series([0])).mean():,.2f}")
        m3.metric("Volume Ativo", len(main_df))

        # --- O BOTAO DE GERAR (Sempre Visível se houver ficheiros) ---
        st.markdown("---")
        if st.button("🚀 GERAR RELATÓRIO ESTRATÉGICO FINAL"):
            with st.spinner('A IA está a cruzar dados da MLS com tendências McKinsey/Zillow...'):
                try:
                    # Agregação de inteligência para a IA
                    stats_data = {
                        "by_zip": main_df.groupby('Zip')['Price'].mean().to_dict() if 'Zip' in main_df.columns else "N/A",
                        "hotspots": main_df['Subdivision'].value_counts().head(10).to_dict() if 'Subdivision' in main_df.columns else "N/A",
                        "zoning": main_df['Zoning'].value_counts().to_dict() if 'Zoning' in main_df.columns else "N/A"
                    }

                    prompt = f"""
                    Aja como um Estrategista de Real Estate da McKinsey e um Investidor Pro.
                    Nível de Análise: {analysis_mode}
                    Dados reais da MLS: {stats_data}
                    Contexto Extra: {master_context}

                    TAREFA:
                    1. OVERVIEW DA CIDADE: Identifique o Condado (Sarasota/Charlotte) e métricas de desemprego/população.
                    2. CMA MODERNO: Determine se os imóveis estão subavaliados usando Média Ponderada.
                    3. FATORES SOCIAIS: Avalie Escolas, Crime e Tendências (Zillow/Redfin/Deloitte).
                    4. ZONEAMENTO E ADU: Com base nas leis da Flórida, identifique potencial para Guest Houses.
                    5. PADRÕES ESCONDIDOS: Cruze preço por SqFt entre diferentes Zipcodes.
                    
                    Escreva em Português de Portugal Profissional.
                    """
                    
                    model = genai.GenerativeModel('gemini-1.5-flash')
                    response = model.generate_content(prompt)
                    st.markdown("### 📊 Relatório de Inteligência Gerado")
                    st.write(response.text)
                    st.balloons()
                except Exception as e:
                    st.error(f"Erro na AI: {e}")

else:
    st.info("💡 Hub Pronto. Arraste os seus ficheiros para ativar o botão de relatório.")
