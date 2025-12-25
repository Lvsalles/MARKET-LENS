import streamlit as st
import pandas as pd
import google.generativeai as genai
import numpy as np
from pypdf import PdfReader
import io

# 1. Configuração de Página
st.set_page_config(page_title="AI Strategic Investor Hub", layout="wide")

# 2. Inicialização Segura da IA (Detecção Automática de Modelo)
if "GOOGLE_API_KEY" not in st.secrets:
    st.error("🔑 ERRO: Adicione a sua GOOGLE_API_KEY nos Secrets do Streamlit.")
    st.stop()

genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])

@st.cache_resource
def get_working_model():
    """Tenta carregar o modelo 1.5-flash, com fallback para gemini-pro se houver erro 404."""
    available_models = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
    # Lista de prioridades
    options = ['models/gemini-1.5-flash', 'models/gemini-1.5-pro', 'models/gemini-pro']
    for opt in options:
        if opt in available_models:
            return genai.GenerativeModel(opt), opt
    return genai.GenerativeModel(available_models[0]), available_models[0]

model, model_name = get_working_model()

# ---------------------------------------------------------
# BIBLIOTECA DE PADRONIZAÇÃO (INVESTOR SYMBOLS)
# ---------------------------------------------------------
SYNONYMS = {
    'Price': ['Current Price', 'Current Price_num', 'Sold Price', 'List Price', 'Price', 'Zestimate'],
    'Status': ['Status', 'Listing Status', 'LSC List Side', 'Status_clean'],
    'Zip': ['Zip', 'Zip Code', 'PostalCode'],
    'Address': ['Address', 'Full Address', 'Street Address'],
    'SqFt': ['Heated Area', 'Heated Area_num', 'SqFt', 'Living Area'],
    'Beds': ['Beds', 'Bedrooms', 'Beds_num'],
    'Baths': ['Full Baths', 'Bathrooms', 'Full Baths_num'],
    'Year': ['Year Built', 'Year Built_num'],
    'DOM': ['CDOM', 'ADOM', 'Days to Contract', 'DOM']
}

def normalize_data(df):
    # Aplica o mapeamento de nomes
    for std, syns in SYNONYMS.items():
        found = next((c for c in syns if c in df.columns), None)
        if found:
            df = df.rename(columns={found: std})
    
    # Remove duplicados de colunas
    df = df.loc[:, ~df.columns.duplicated(keep='last')]
    
    # Limpeza de preços e números
    if 'Price' in df.columns:
        df['Price'] = pd.to_numeric(df['Price'].astype(str).str.replace(r'[$,]', '', regex=True), errors='coerce')
    if 'SqFt' in df.columns:
        df['Price_SqFt'] = df['Price'] / df['SqFt']
    
    return df

# ---------------------------------------------------------
# INTERFACE PRINCIPAL
# ---------------------------------------------------------
st.sidebar.title("💎 Painel do Estrategista")
analysis_mode = st.sidebar.selectbox(
    "Nível de Inteligência",
    ["Análise de Arbitragem", "CMA Moderno (Média Ponderada)", "Estratégia Macro & ROI"]
)
st.sidebar.caption(f"Motor Ativo: `{model_name}`")

st.title("🏙️ Global Real Estate Investment Hub")
st.markdown("---")

# Upload de Ficheiros
uploaded_files = st.file_uploader("Arraste os arquivos MLS (CSV, XLSX, PDF)", accept_multiple_files=True)

if uploaded_files:
    master_context = ""
    all_dfs = []

    for f in uploaded_files:
        ext = f.name.split('.')[-1].lower()
        with st.expander(f"📁 Lendo: {f.name}"):
            try:
                if ext in ['csv', 'xlsx']:
                    df = pd.read_csv(f) if ext == 'csv' else pd.read_excel(f)
                    df = normalize_data(df)
                    all_dfs.append(df)
                    st.success("Variáveis mapeadas.")
                elif ext == 'pdf':
                    reader = PdfReader(f)
                    master_context += f"\n[DOCUMENTO: {f.name}]\n" + " ".join([p.extract_text() for p in reader.pages[:5]])
            except Exception as e:
                st.error(f"Erro: {e}")

    if all_dfs:
        main_df = pd.concat(all_dfs, ignore_index=True)
        st.write("### 📊 Amostragem dos Dados Normalizados")
        st.dataframe(main_df.head(10))

        # --- BOTÃO DE RELATÓRIO ---
        st.markdown("---")
        if st.button("🚀 GERAR RELATÓRIO ESTRATÉGICO"):
            with st.spinner('A IA está a processar os dados...'):
                try:
                    # Preparar resumo para a IA
                    stats_summary = main_df.describe().to_string()
                    zips = main_df['Zip'].value_counts().head(5).to_dict() if 'Zip' in main_df.columns else {}
                    
                    prompt = f"""
                    Você é um Especialista de Investimento Imobiliário da McKinsey.
                    Modo: {analysis_mode}
                    
                    DADOS MLS: {stats_summary}
                    ZIP CODES HOTSPOTS: {zips}
                    CONTEXTO EXTRA: {master_context[:2000]}

                    TAREFA:
                    1. Analise quartos, banheiros, piscina e SqFt para achar subvalorizados.
                    2. Compare preços entre diferentes Zip Codes.
                    3. Integre conhecimentos de Escolas, Crime e Economia local (North Port/Venice).
                    4. Cite tendências Zillow/Redfin/Deloitte para 2025.
                    5. Identifique potencial de ADU (Guest Houses) baseando-se no zoneamento.

                    Responda em Português de Portugal com tom executivo.
                    """
                    
                    response = model.generate_content(prompt)
                    st.markdown("### 📊 Relatório de Inteligência Gerado")
                    st.write(response.text)
                    st.balloons()
                except Exception as e:
                    st.error(f"Erro na geração: {e}")
else:
    st.info("💡 Por favor, carregue os ficheiros para ativar a análise.")
