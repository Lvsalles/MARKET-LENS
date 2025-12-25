import streamlit as st
import pandas as pd
import google.generativeai as genai
from pypdf import PdfReader
from docx import Document
import numpy as np

# 1. Configuração de Página e Estilo
st.set_page_config(page_title="AI Market Lens Hub", layout="wide")

# 2. Inicialização Segura da API
if "GOOGLE_API_KEY" not in st.secrets:
    st.error("🔑 API Key em falta. Adicione GOOGLE_API_KEY nos Secrets do Streamlit.")
    st.stop()

genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])

# 3. Biblioteca Universal de Padronização (Mapeamento de Sinónimos)
SYNONYMS_LIB = {
    'columns': {
        'Current Price': 'Price', 'Current Price_num': 'Price', 'List Price': 'Price', 'Sold Price': 'Price',
        'Zestimate': 'Price', 'Redfin Estimate': 'Price',
        'Legal Subdivision Name': 'Subdivision', 'Subdivision/Condo Name': 'Subdivision',
        'Heated Area': 'SqFt', 'Heated Area_num': 'SqFt', 'Living Area': 'SqFt',
        'CDOM': 'DOM', 'ADOM': 'DOM', 'Days to Contract': 'DOM',
        'Status_clean': 'Status', 'LSC List Side': 'Status', 'Listing Status': 'Status',
        'Address': 'Address', 'Full Address': 'Address', 'Street Address': 'Address'
    },
    'status_values': {
        'ACT': 'Active', 'Active': 'Active', 'A': 'Active',
        'SLD': 'Sold', 'Sold': 'Sold', 'S': 'Sold', 'Closed': 'Sold',
        'PND': 'Pending', 'Pending': 'Pending', 'P': 'Pending'
    }
}

# 4. Funções de Extração de Dados
def parse_docx(file):
    doc = Document(file)
    return "\n".join([p.text for p in doc.paragraphs])

def process_file(uploaded_file):
    name = uploaded_file.name.lower()
    ext = name.split('.')[-1]
    
    if ext == 'pdf':
        reader = PdfReader(uploaded_file)
        return " ".join([p.extract_text() for p in reader.pages[:10]]), "PDF_Document"
    elif ext == 'docx':
        return parse_docx(uploaded_file), "Word_Document"
    
    # Processamento de Planilhas (CSV/XLSX)
    df = pd.read_csv(uploaded_file) if ext == 'csv' else pd.read_excel(uploaded_file)
    
    # Padronização de Colunas
    df = df.rename(columns={k: v for k, v in SYNONYMS_LIB['columns'].items() if k in df.columns})
    df = df.loc[:, ~df.columns.duplicated(keep='last')]
    
    if 'Status' in df.columns:
        df['Status'] = df['Status'].map(SYNONYMS_LIB['status_values']).fillna(df['Status'])
    
    if 'Price' in df.columns:
        df['Price'] = pd.to_numeric(df['Price'].astype(str).str.replace(r'[$,]', '', regex=True), errors='coerce')

    # Identificação de Categoria
    cat = "Residential"
    if "land" in name or "acreage" in str(df.columns).lower(): cat = "Land"
    elif "rent" in name or "lease" in str(df.columns).lower(): cat = "Rental"
    elif any(p in name for p in ["zillow", "redfin", "realtor"]): cat = "Portal_Data"
    
    return df, cat

# 5. Interface Lateral (Sidebar) - Filtros e Níveis
st.sidebar.header("🔍 Configuração da Análise")
analysis_mode = st.sidebar.selectbox(
    "Nível de Detalhe", 
    ["Análise de Mercado Global", "Análise por Endereço/Propriedade", "Análise de Portais (Zillow/Redfin)", "Desempenho de Agentes"]
)

# 6. Interface Principal
st.title("🏙️ Real Estate Intelligence Station")
st.markdown("---")

files = st.file_uploader("Arraste os seus arquivos (CSV, XLSX, PDF, DOCX)", accept_multiple_files=True)

if files:
    full_data_context = f"MODO DE ANÁLISE: {analysis_mode}\n\n"
    
    for f in files:
        with st.expander(f"📁 Processando: {f.name}"):
            res, category = process_file(f)
            
            if isinstance(res, pd.DataFrame):
                # Análise Variável por Variável
                stats = {
                    "Total_Linhas": len(res),
                    "Preço_Médio": res['Price'].mean() if 'Price' in res.columns else 0,
                    "Top_Subdivisões": res['Subdivision'].value_counts().head(5).to_dict() if 'Subdivision' in res.columns else {},
                    "Status_Distribuição": res['Status'].value_counts().to_dict() if 'Status' in res.columns else {}
                }
                full_data_context += f"\n--- FONTE ({category}): {f.name} ---\nEstatísticas: {stats}\nAmostra:\n{res.head(30).to_string()}\n"
                st.write(f"Categoria: **{category}**")
                st.write(stats)
            else:
                full_data_context += f"\n--- DOCUMENTO: {f.name} ---\n{res[:3000]}\n"
                st.success("Texto extraído do documento.")

    if st.button("🚀 Gerar Relatório Estratégico"):
        with st.spinner('A IA está a cruzar todas as fontes de dados...'):
            try:
                # Prompt flexível para permitir que a IA "pense" por si mesma
                prompt = f"""
                Age como um Consultor de Investimentos Imobiliários de Elite na Flórida.
                Utiliza os dados padronizados abaixo para criar um relatório estratégico.
                
                DADOS PROCESSADOS:
                {full_data_context}
                
                OBJETIVO:
                Executa uma análise de nível "{analysis_mode}". 
                Cruza informações de portais (Zillow/Redfin) com dados reais da MLS se disponíveis.
                Identifica discrepâncias de preços, velocidade de vendas (Sold vs Active) e hotspots geográficos.
                
                ESTRUTURA DO RELATÓRIO:
                1. SUMÁRIO EXECUTIVO (The "Why"): O que os dados realmente significam hoje?
                2. ANÁLISE DE VELOCIDADE E PREÇO: Como está o inventário vs vendas?
                3. INSIGHTS POR ENDEREÇO/ZONA: Onde está o lucro?
                4. RECOMENDAÇÕES ESTRATÉGICAS: 5 pontos acionáveis para o investidor.
                
                Escreve em Inglês Profissional. Usa Markdown.
                """
                
                model = genai.GenerativeModel('gemini-1.5-flash')
                response = model.generate_content(prompt)
                st.markdown("---")
                st.markdown("### 📊 Relatório de Inteligência Gerado")
                st.write(response.text)
                st.balloons()
            except Exception as e:
                st.error(f"Erro na análise: {e}")
