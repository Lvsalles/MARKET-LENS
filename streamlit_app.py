import streamlit as st
import pandas as pd
import google.generativeai as genai
from pypdf import PdfReader
import io

# 1. Configuração de Segurança (Secrets)
try:
    genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])
    model = genai.GenerativeModel('gemini-1.5-flash')
except Exception as e:
    st.error("Erro na Chave API. Verifique os Secrets no Streamlit Cloud.")

st.set_page_config(page_title="Market Analyst Pro", layout="wide")
st.title("🤖 Analista Imobiliário IA")
st.write("Suba arquivos CSV, Excel ou PDF para análise de mercado em North Port, Venice e região.")

# 2. Upload de Arquivo (Aceita múltiplos formatos)
uploaded_file = st.file_uploader("Arraste seu arquivo aqui", type=['csv', 'xlsx', 'pdf'])

if uploaded_file:
    content_to_analyze = ""
    
    # Se for PDF
    if uploaded_file.name.endswith('.pdf'):
        pdf_reader = PdfReader(uploaded_file)
        text = ""
        for page in pdf_reader.pages:
            text += page.extract_text()
        content_to_analyze = text[:10000] # Limite para não travar
        st.success("PDF lido com sucesso!")
        
    # Se for Excel ou CSV
    else:
        try:
            if uploaded_file.name.endswith('.csv'):
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file)
            
            st.success("Tabela carregada!")
            st.dataframe(df.head(5)) # Mostra as primeiras 5 linhas
            content_to_analyze = df.head(30).to_string() # Envia uma amostra para a IA
        except Exception as e:
            st.error(f"Erro ao ler tabela: {e}")

    # 3. Botão de Análise
    if st.button("🔍 Realizar Análise Estratégica"):
        if content_to_analyze:
            with st.spinner('A IA está processando os dados...'):
                prompt = f"""
                Analise os seguintes dados imobiliários de North Port/Venice:
                {content_to_analyze}
                
                Por favor, forneça:
                1. Um resumo geral do que se trata o arquivo.
                2. Destaques de preços (médias, imóveis mais caros/baratos).
                3. Identificação de oportunidades baseadas em localização (Subdivisions).
                4. Recomendação estratégica para investidores.
                Responda em Português.
                """
                
                response = model.generate_content(prompt)
                st.markdown("### 📋 Relatório de Inteligência de Mercado")
                st.write(response.text)
        else:
            st.warning("Por favor, suba um arquivo primeiro.")
