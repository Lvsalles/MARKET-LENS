import streamlit as st
import pandas as pd
import google.generativeai as genai
import numpy as np
from pypdf import PdfReader

# 1. Configuração Básica (Sempre visível)
st.set_page_config(page_title="Investidor Pro Hub", layout="wide")

# 2. Configuração da IA
if "GOOGLE_API_KEY" in st.secrets:
    genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])
else:
    st.error("🔑 Configure a sua API KEY nos Secrets do Streamlit!")
    st.stop()

# ---------------------------------------------------------
# BIBLIOTECA DE PADRONIZAÇÃO (Dicionário de Sinónimos)
# ---------------------------------------------------------
SYNONYMS = {
    'Price': ['Current Price', 'Current Price_num', 'Sold Price', 'List Price', 'Price'],
    'Status': ['Status', 'Listing Status', 'LSC List Side', 'Status_clean'],
    'Zip': ['Zip', 'Zip Code', 'PostalCode'],
    'Address': ['Address', 'Full Address', 'Street Address'],
    'SqFt': ['Heated Area', 'Heated Area_num', 'SqFt', 'Living Area'],
    'Beds': ['Beds', 'Bedrooms', 'Beds_num'],
    'Baths': ['Full Baths', 'Bathrooms', 'Full Baths_num'],
    'DOM': ['CDOM', 'ADOM', 'Days to Contract', 'DOM'],
    'Subdivision': ['Legal Subdivision Name', 'Subdivision/Condo Name']
}

def clean_data(df):
    for std, syns in SYNONYMS.items():
        found = next((c for c in syns if c in df.columns), None)
        if found: df = df.rename(columns={found: std})
    if 'Price' in df.columns:
        df['Price'] = pd.to_numeric(df['Price'].astype(str).str.replace(r'[$,]', '', regex=True), errors='coerce')
    return df

# ---------------------------------------------------------
# INTERFACE PRINCIPAL
# ---------------------------------------------------------
st.title("🏙️ Sistema de Inteligência Imobiliária")
st.sidebar.header("Painel de Controlo")
analise_tipo = st.sidebar.selectbox("Tipo de Relatório", ["Macro Economia", "CMA Moderno", "Zoneamento & ROI"])

# Upload de Ficheiros
files = st.file_uploader("Suba os seus ficheiros MLS (CSV, XLSX ou PDF)", accept_multiple_files=True)

# Memória de dados
all_dfs = []
contexto_texto = ""

if files:
    for f in files:
        ext = f.name.split('.')[-1].lower()
        try:
            if ext in ['csv', 'xlsx']:
                df = pd.read_csv(f) if ext == 'csv' else pd.read_excel(f)
                all_dfs.append(clean_data(df))
                st.sidebar.success(f"✅ {f.name} carregado")
            elif ext == 'pdf':
                text = " ".join([p.extract_text() for p in PdfReader(f).pages[:5]])
                contexto_texto += f"\n[DOCUMENTO: {f.name}]\n{text[:1000]}"
        except Exception as e:
            st.error(f"Erro ao ler {f.name}: {e}")

    # Mostrar Dados se existirem
    if all_dfs:
        main_df = pd.concat(all_dfs, ignore_index=True)
        st.write("### 📊 Visão Geral dos Dados")
        st.dataframe(main_df.head(5))
        
        # Métricas Rápidas
        c1, c2 = st.columns(2)
        c1.metric("Total de Registos", len(main_df))
        if 'Price' in main_df.columns:
            c2.metric("Preço Médio", f"${main_df['Price'].mean():,.0f}")

    # --- BOTÃO DE RELATÓRIO (POSIÇÃO FIXA) ---
    st.markdown("---")
    st.subheader("🚀 Gerador de Inteligência")
    
    if st.button("GERAR RELATÓRIO ESTRATÉGICO"):
        with st.spinner('A IA está a pensar...'):
            try:
                # Resumo para a IA
                resumo = main_df.describe().to_string() if all_dfs else "Apenas documentos de texto."
                
                prompt = f"""
                Age como um Especialista em Investimento Imobiliário. 
                Análise: {analise_tipo}
                Dados: {resumo}
                Extra: {contexto_texto}
                
                Faz uma análise variável por variável (quartos, banheiros, piscina, etc).
                Cruza com tendências da Zillow, Redfin e McKinsey.
                Fala sobre Escolas, Crime e Zoneamento (ADU) em North Port/Venice.
                Dê 5 recomendações de investimento.
                """
                
                model = genai.GenerativeModel('gemini-1.5-flash')
                response = model.generate_content(prompt)
                st.markdown("### 📊 Relatório Final")
                st.write(response.text)
            except Exception as e:
                st.error(f"Erro na IA: {e}")

else:
    st.info("💡 Arraste os ficheiros para aqui para começar.")
