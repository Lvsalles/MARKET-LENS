import streamlit as st
import pandas as pd
import google.generativeai as genai
import numpy as np
from pypdf import PdfReader
from docx import Document

# 1. Configuração de Alta Performance
st.set_page_config(page_title="AI Investor Hub 360", layout="wide")

# 2. Inicialização da IA com Verificação de Segurança
if "GOOGLE_API_KEY" not in st.secrets:
    st.error("🔑 API Key faltando nos Secrets do Streamlit.")
    st.stop()

genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])

# 3. Biblioteca de Padronização Universal (Dicionário de Sinônimos)
# Esta biblioteca une colunas de diferentes fontes (MLS, Zillow, Redfin)
MAPPING = {
    'Price': ['Current Price', 'Current Price_num', 'List Price', 'Sold Price', 'Price'],
    'Status': ['Status', 'LSC List Side', 'Listing Status'],
    'Address': ['Address', 'Full Address', 'Street Address'],
    'Zip': ['Zip', 'Zip Code', 'PostalCode'],
    'SqFt': ['Heated Area', 'Heated Area_num', 'SqFt', 'Living Area'],
    'Beds': ['Beds', 'Bedrooms', 'Beds_num'],
    'Baths': ['Full Baths', 'Bathrooms', 'Full Baths_num'],
    'Garage': ['Garage', 'Garage Spaces', 'Carport'],
    'Pool': ['Pool', 'Pool Private', 'Pool Features'],
    'Year': ['Year Built', 'Year Built_num'],
    'DOM': ['CDOM', 'ADOM', 'Days to Contract', 'DOM'],
    'Financing': ['Sold Terms', 'Terms', 'Financing'],
    'Zoning': ['Zoning', 'Zoning Code', 'Land Use']
}

def standardize_data(df):
    for std_col, syns in MAPPING.items():
        found = next((c for c in df.columns if c in syns), None)
        if found:
            df = df.rename(columns={found: std_col})
    # Limpeza de dados numéricos
    for col in ['Price', 'SqFt', 'Beds', 'Baths', 'DOM']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(r'[$,]', '', regex=True), errors='coerce')
    return df

# 4. Interface Lateral (Sidebar)
st.sidebar.title("🏢 Painel do Estrategista")
analysis_mode = st.sidebar.selectbox(
    "Nível de Inteligência",
    ["Análise de Arbitragem (Subvalorizados)", "CMA Moderno & ROI", "Zoneamento & Potencial ADU", "Estudo de Escolas e Criminalidade"]
)

# 5. Interface Principal
st.title("🏙️ Global Real Estate Investment Hub")
st.markdown("---")

uploaded_files = st.file_uploader("Arraste os arquivos MLS, Land, Rental ou Portais aqui", accept_multiple_files=True)

# Lógica do Botão: Ele deve aparecer FORA do loop de upload para estar sempre visível se houver arquivos
if uploaded_files:
    all_dfs = []
    text_context = ""

    for f in uploaded_files:
        ext = f.name.split('.')[-1].lower()
        with st.expander(f"📁 Lendo: {f.name}"):
            try:
                if ext in ['csv', 'xlsx']:
                    df = pd.read_csv(f) if ext == 'csv' else pd.read_excel(f)
                    df = standardize_data(df)
                    all_dfs.append(df)
                    st.success("Dados estruturados e mapeados.")
                elif ext == 'pdf':
                    text_context += " ".join([p.extract_text() for p in PdfReader(f).pages[:5]])
            except Exception as e:
                st.error(f"Erro: {e}")

    if all_dfs:
        main_df = pd.concat(all_dfs, ignore_index=True)
        # Dashboard Rápido
        c1, c2, c3 = st.columns(3)
        c1.metric("Database Total", len(main_df))
        c2.metric("Preço Médio", f"${main_df['Price'].mean():,.0f}")
        c3.metric("Média $/SqFt", f"${(main_df['Price']/main_df['SqFt']).mean():,.2f}")

    # --- O BOTAO DE GERAR AGORA ESTA PROTEGIDO E VISÍVEL ---
    st.markdown("---")
    if st.button("🚀 GERAR RELATÓRIO DE INTELIGÊNCIA MASTER"):
        with st.spinner('AI cruzando variáveis, zoneamento e tendências globais...'):
            try:
                # Criar resumo de 100% dos dados para a IA
                stats_summary = main_df.describe().to_string() if all_dfs else "Dados de texto apenas."
                hotspots = main_df['Zip'].value_counts().head(5).to_dict() if 'Zip' in main_df.columns else {}
                
                prompt = f"""
                Aja como um Investidor de Real Estate e Especialista da McKinsey.
                Modo de Análise: {analysis_mode}
                
                ESTATÍSTICAS DOS ARQUIVOS: {stats_summary}
                HOTSPOTS POR ZIP: {hotspots}
                CONTEXTO EXTRA (PDF/WORD): {text_context[:2000]}

                SUA TAREFA:
                1. MICRO-MARKET: Analise variável por variável (Beds, Baths, Pool, Garage). Onde está o lucro?
                2. ARBITRAGEM: Identifique Zip Codes ou ruas onde o preço/SqFt está abaixo da média.
                3. FINANCIAMENTO: Cruze 'Sold Terms' (Cash vs Conv) com a velocidade de venda.
                4. CONTEXTO EXTERNO: Use seus dados sobre Escolas, Crime e Economia de North Port/Venice.
                5. TENDÊNCIAS: Cruze com insights da Zillow, Redfin, Deloitte e McKinsey para 2025.
                6. ZONEAMENTO: Identifique potencial de ADU (Guest Houses) para maximizar ROI.

                Relatório em Português Profissional com links de busca para o Google Maps.
                """
                
                model = genai.GenerativeModel('gemini-1.5-flash')
                response = model.generate_content(prompt)
                st.markdown("### 📊 Relatório Estratégico do Investidor")
                st.write(response.text)
                st.balloons()
            except Exception as e:
                st.error(f"Erro na geração: {e}")
else:
    st.info("💡 Hub Ativo. Carregue os arquivos para habilitar o motor de inteligência.")
