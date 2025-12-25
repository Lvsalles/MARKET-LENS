import streamlit as st
import pandas as pd
import google.generativeai as genai
import numpy as np
from pypdf import PdfReader

# 1. Configuração de Página
st.set_page_config(page_title="AI Market Intelligence", layout="wide")

# 2. Inicialização Inteligente da IA (Evita Erro 404)
if "GOOGLE_API_KEY" not in st.secrets:
    st.error("🔑 ERRO: Adicione a sua GOOGLE_API_KEY nos Secrets do Streamlit.")
    st.stop()

genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])

def get_available_model():
    """Deteta automaticamente o melhor modelo disponível para evitar erro 404."""
    try:
        models = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
        # Preferência por Flash 1.5, depois Pro, depois o que estiver disponível
        for m in ['models/gemini-1.5-flash', 'models/gemini-1.5-pro', 'models/gemini-pro']:
            if m in models: return m
        return models[0]
    except Exception:
        return "models/gemini-pro" # Fallback padrão

# 3. Biblioteca de Padronização (Synonyms)
SYNONYMS = {
    'Price': ['Current Price', 'Current Price_num', 'Sold Price', 'List Price', 'Price'],
    'Status': ['Status', 'Listing Status', 'LSC List Side', 'Status_clean'],
    'Zip': ['Zip', 'Zip Code', 'PostalCode'],
    'Address': ['Address', 'Full Address', 'Street Address'],
    'SqFt': ['Heated Area', 'Heated Area_num', 'SqFt', 'Living Area'],
    'Beds': ['Beds', 'Bedrooms', 'Beds_num'],
    'Baths': ['Full Baths', 'Bathrooms', 'Full Baths_num'],
    'DOM': ['CDOM', 'ADOM', 'Days to Contract', 'DOM']
}

def clean_data(df):
    for std, syns in SYNONYMS.items():
        found = next((c for c in syns if c in df.columns), None)
        if found: df = df.rename(columns={found: std})
    if 'Price' in df.columns:
        df['Price'] = pd.to_numeric(df['Price'].astype(str).str.replace(r'[$,]', '', regex=True), errors='coerce')
    return df

# 4. Interface Sidebar
st.sidebar.title("💎 Painel do Investidor")
analise_tipo = st.sidebar.selectbox("Foco da Análise", ["ROI & Arbitragem", "CMA Moderno", "Zoneamento & Escolas"])

# 5. Interface Principal
st.title("🏙️ Sistema de Inteligência Imobiliária")
st.markdown("---")

files = st.file_uploader("Suba aqui os seus dados (MLS, Land, Rental ou PDF)", accept_multiple_files=True)

master_context = ""
all_dfs = []

if files:
    for f in files:
        ext = f.name.split('.')[-1].lower()
        try:
            if ext in ['csv', 'xlsx']:
                df = pd.read_csv(f) if ext == 'csv' else pd.read_excel(f)
                all_dfs.append(clean_data(df))
                st.sidebar.success(f"✅ {f.name} lido")
            elif ext == 'pdf':
                text = " ".join([p.extract_text() for p in PdfReader(f).pages[:5]])
                master_context += f"\n[DOC: {f.name}]\n{text[:1500]}"
        except Exception as e:
            st.error(f"Erro ao processar {f.name}: {e}")

    if all_dfs:
        main_df = pd.concat(all_dfs, ignore_index=True)
        st.write("### 📊 Amostragem de Dados")
        st.dataframe(main_df.head(5))
        
        # Métricas em tempo real
        c1, c2, c3 = st.columns(3)
        c1.metric("Database Total", len(main_df))
        if 'Price' in main_df.columns:
            c2.metric("Preço Médio", f"${main_df['Price'].mean():,.0f}")
        c3.metric("Zip Codes", main_df['Zip'].nunique() if 'Zip' in main_df.columns else "N/A")

    # --- BOTÃO DE RELATÓRIO (POSIÇÃO FIXA) ---
    st.markdown("---")
    st.subheader("🚀 Gerador de Estratégia")
    
    if st.button("GERAR RELATÓRIO AGORA"):
        with st.spinner('A IA está a analisar o mercado...'):
            try:
                target_model = get_available_model()
                st.caption(f"Utilizando motor: `{target_model}`")
                
                resumo = main_df.describe().to_string() if all_dfs else "Apenas texto."
                
                prompt = f"""
                Você é um Especialista em Investimento Imobiliário da McKinsey.
                Objetivo: {analise_tipo}
                Dados MLS: {resumo}
                Extra: {master_context}
                
                TAREFA:
                1. Analise quartos, banheiros, piscina e SqFt.
                2. Cruze com tendências Zillow/McKinsey.
                3. Analise Escolas, Crime e Zoneamento em North Port/Venice.
                4. Liste 5 recomendações com links (placeholders) para o Google Maps.
                """
                
                model = genai.GenerativeModel(target_model)
                response = model.generate_content(prompt)
                st.markdown("### 📊 Relatório Estratégico")
                st.write(response.text)
                st.balloons()
            except Exception as e:
                st.error(f"Erro na IA: {e}")
else:
    st.info("💡 Por favor, suba um ficheiro para começar.")
