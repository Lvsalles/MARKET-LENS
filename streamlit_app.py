import streamlit as st
import pandas as pd
import google.generativeai as genai
from pypdf import PdfReader

# Configuração da IA
genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])
model = genai.GenerativeModel('gemini-1.5-flash')

st.title("🤖 Analista de Mercado Multiformato")
st.write("Envia ficheiros CSV, Excel ou PDF para análise.")

# Alteramos aqui para aceitar os novos formatos
uploaded_file = st.file_uploader("Escolha o ficheiro", type=['csv', 'xlsx', 'pdf'])

if uploaded_file:
    texto_para_analise = ""
    nome_ficheiro = uploaded_file.name

    # LÓGICA PARA PDF
    if nome_ficheiro.endswith('.pdf'):
        reader = PdfReader(uploaded_file)
        # Extrai o texto de todas as páginas
        for page in reader.pages:
            texto_para_analise += page.extract_text()
        st.success("PDF lido com sucesso!")
        st.text_area("Conteúdo extraído do PDF", texto_para_analise[:500] + "...", height=150)

    # LÓGICA PARA EXCEL OU CSV
    else:
        if nome_ficheiro.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)
        
        st.success("Tabela carregada com sucesso!")
        st.dataframe(df.head(10))
        texto_para_analise = df.head(30).to_string()

    # BOTÃO DE ANÁLISE (Funciona para qualquer um dos 3 formatos)
    if st.button("🚀 Iniciar Análise Inteligente"):
        with st.spinner('O Gemini está a processar o conteúdo...'):
            prompt = f"Analisa este documento de mercado e extrai os pontos principais, tendências e recomendações: \n\n {texto_para_analise}"
            
            response = model.generate_content(prompt)
            st.markdown("### 📊 Relatório Final")
            st.write(response.text)
