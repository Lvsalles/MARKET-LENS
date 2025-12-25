import streamlit as st
import pandas as pd
import google.generativeai as genai
from pypdf import PdfReader
import numpy as np

# 1. Configuração de Página
st.set_page_config(page_title="AI Market Intelligence", layout="wide")

# 2. Biblioteca de Padronização (The Brain)
# Mapeia diferentes nomes de colunas para um padrão único para a análise
COLUMN_MAPPING = {
    'Price': ['Current Price', 'Current Price_num', 'List Price', 'Sold Price', 'Price'],
    'Status': ['Status', 'Status_clean', 'LSC List Side', 'Status_norm'],
    'Subdivision': ['Legal Subdivision Name', 'Subdivision/Condo Name', 'Subdivision'],
    'City': ['City'],
    'Zip': ['Zip', 'Zip Code'],
    'Beds': ['Beds', 'Beds_num', 'Bedrooms'],
    'Baths': ['Full Baths', 'Full Baths_num', 'Bathrooms'],
    'SqFt': ['Heated Area', 'Heated Area_num', 'SqFt', 'Living Area'],
    'YearBuilt': ['Year Built', 'Year Built_num'],
    'DOM': ['CDOM', 'ADOM', 'Days to Contract', 'DOM', 'Days to Contract_num'],
    'LotSize': ['Total Acreage', 'Total Acreage_num', 'Lot Size Square Footage', 'Lot Size Square Footage_num']
}

STATUS_MAPPING = {
    'ACT': 'Active', 'Active': 'Active', 'A': 'Active',
    'SLD': 'Sold', 'Sold': 'Sold', 'S': 'Sold', 'Closed': 'Sold',
    'PND': 'Pending', 'Pending': 'Pending', 'P': 'Pending', 'Under Contract': 'Pending'
}

# 3. Inicialização Segura da API
if "GOOGLE_API_KEY" not in st.secrets:
    st.error("🔑 API Key missing in Streamlit Secrets.")
    st.stop()

genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])

@st.cache_resource
def get_best_model():
    """Descobre automaticamente qual modelo o seu acesso permite usar para evitar o erro 404."""
    try:
        available = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
        prefs = ['models/gemini-1.5-flash', 'models/gemini-1.5-pro', 'models/gemini-pro']
        for p in prefs:
            if p in available: return genai.GenerativeModel(p), p
        return genai.GenerativeModel(available[0]), available[0]
    except: return None, None

model, model_name = get_best_model()

# 4. Funções de Processamento
def normalize_dataframe(df, filename):
    # Identifica Categoria
    name_low = filename.lower()
    category = "Residential"
    if "land" in name_low or "lots" in name_low: category = "Land"
    elif "rent" in name_low or "lease" in name_low: category = "Rental"

    # Aplica Biblioteca de Padronização
    new_df = df.copy()
    for standard_name, synonyms in COLUMN_MAPPING.items():
        found = next((c for c in synonyms if c in df.columns), None)
        if found:
            # Se houver uma versão numérica (_num), prefere ela
            num_version = next((c for c in df.columns if c == found + "_num"), found)
            new_df[standard_name] = df[num_version]

    # Remove duplicatas de colunas renomeadas
    new_df = new_df.loc[:, ~new_df.columns.duplicated()]

    # Traduz Status
    if 'Status' in new_df.columns:
        new_df['Status'] = new_df['Status'].map(STATUS_MAPPING).fillna(new_df['Status'])
    
    # Limpa Preços
    if 'Price' in new_df.columns:
        new_df['Price'] = pd.to_numeric(new_df['Price'].astype(str).str.replace(r'[$,]', '', regex=True), errors='coerce')

    return new_df, category

def get_variable_analysis(df):
    """Realiza análise variável por variável para alimentar a IA."""
    analysis = {}
    for col in ['Price', 'Beds', 'Baths', 'SqFt', 'DOM', 'LotSize', 'YearBuilt']:
        if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
            analysis[col] = {
                "Average": round(df[col].mean(), 2),
                "Min": df[col].min(),
                "Max": df[col].max()
            }
    if 'Status' in df.columns:
        analysis['Market_Status'] = df['Status'].value_counts().to_dict()
    if 'Subdivision' in df.columns:
        analysis['Top_Hotspots'] = df['Subdivision'].value_counts().head(10).to_dict()
    return analysis

# 5. Interface Streamlit
st.title("🤖 AI Strategic Market Analyst")
st.subheader("Global Multi-Source Intelligence System")
if model_name: st.caption(f"Engine: `{model_name}`")
st.markdown("---")

uploaded_files = st.file_uploader("Upload MLS, Land or Rental files (CSV/XLSX/PDF)", accept_multiple_files=True)

if uploaded_files:
    aggregated_intel = ""
    
    for f in uploaded_files:
        with st.expander(f"Analyzing Variables: {f.name}"):
            if f.name.endswith('.pdf'):
                reader = PdfReader(f)
                text = " ".join([p.extract_text() for p in reader.pages[:5]])
                aggregated_intel += f"\n[DOCUMENT: {f.name}]\n{text[:3000]}\n"
                st.info("PDF Content Extracted.")
            else:
                df_raw = pd.read_csv(f) if f.name.endswith('.csv') else pd.read_excel(f)
                df, cat = normalize_dataframe(df_raw, f.name)
                var_analysis = get_variable_analysis(df)
                
                # Alimenta o contexto da IA
                aggregated_intel += f"\n[CATEGORY: {cat} | FILE: {f.name}]\nDetailed Variables: {var_analysis}\n"
                
                st.success(f"Category Identified: {cat}")
                st.write("Variable Summary:", var_analysis)
                st.dataframe(df.head(5))

    if st.button("🚀 Generate Strategic Executive Report"):
        with st.spinner("AI is thinking independently..."):
            try:
                prompt = f"""
                You are a Senior Real Estate Investment Strategist.
                I have processed multiple files and standardized the variables for you. 
                Below is the Deep Variable Analysis for all provided data:
                
                {aggregated_intel}
                
                YOUR TASK:
                1. Think independently: Do not just repeat the numbers. Analyze the "WHY". 
                2. Status Logic: Compare Sold vs Active inventory to identify market velocity.
                3. Market Gaps: Is there an oversupply of Land compared to Residential demand? 
                4. Geographic Alpha: Which Subdivisions offer the best risk/reward based on the data?
                5. Executive Strategy: Provide 5 high-level professional recommendations in English.

                Format: Use professional headers, bullet points, and a sharp investment tone.
                """
                
                response = model.generate_content(prompt)
                st.markdown("### 📊 Global Market Intelligence Report")
                st.write(response.text)
                st.balloons()
            except Exception as e:
                st.error(f"AI error: {e}")
else:
    st.info("💡 Pro Tip: Upload all files together (MLS listings, Land data, and Rentals) for a comparative study.")
