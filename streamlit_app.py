import streamlit as st
import pandas as pd
import google.generativeai as genai
from pypdf import PdfReader
import io

# 1. Configuração de Inicialização e Segurança
st.set_page_config(page_title="AI Market Analyst Pro", layout="wide")

# Tentar configurar a API através do Secrets
if "GOOGLE_API_KEY" not in st.secrets:
    st.error("ERRO: Chave API não encontrada nos Secrets do Streamlit.")
    st.stop()

genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])
model = genai.GenerativeModel('gemini-1.5-flash')

# Cabeçalho da Ferramenta
st.title("🤖 Analista Imobiliário Inteligente")
st.write("Suporte para: **CSV, Excel (.xlsx) e PDF**")
st.markdown("---")

# 2. Upload de Arquivos
uploaded_file = st.file_uploader("Arraste ou selecione o arquivo para análise", type=['csv', 'xlsx', 'pdf'])

if uploaded_file:
    content_to_analyze = ""
    file_type = uploaded_file.name.split('.')[-1].lower()

    try:
        # LÓGICA PARA PDF
        if file_type == 'pdf':
            reader = PdfReader(uploaded_file)
            pdf_text = ""
            # Lemos apenas as primeiras 10 páginas para evitar erro de tamanho
            for i, page in enumerate(reader.pages[:10]):
                pdf_text += page.extract_text()
            content_to_analyze = pdf_text
            st.success("✅ PDF carregado com sucesso!")
            st.info("Resumo do conteúdo detectado no PDF:")
            st.text(content_to_analyze[:300] + "...")

        # LÓGICA PARA EXCEL OU CSV
        else:
            if file_type == 'csv':
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file)
            
            st.success("✅ Planilha carregada com sucesso!")
            st.subheader("Prévia dos Dados (Top 5 linhas)")
            st.dataframe(df.head(5))
            
            # Convertemos apenas as primeiras 15 linhas para texto 
            # para evitar o erro 'InvalidArgument' (limite de tamanho)
            content_to_analyze = df.head(15).to_string()

        # 3. Botão de Execução da IA
        st.markdown("---")
        if st.button("🚀 Iniciar Análise com IA"):
            with st.spinner('A IA está processando os dados e gerando insights...'):
                try:
                    # Criamos o comando (Prompt) para a IA
                    prompt = f"""
                    Você é um especialista em análise de dados e mercado imobiliário da Flórida.
                    Analise o conteúdo abaixo extraído do arquivo {uploaded_file.name}:
                    
                    {content_to_analyze}
                    
                    Com base nesses dados, gere um relatório profissional contendo:
                    1. Resumo do tipo de dado (é uma lista de imóveis, terrenos, relatório de vendas?).
                    2. Análise de preços (média de valor, o mais caro e o mais barato).
                    3. Localizações em destaque (Cidades ou Subdivisions).
                    4. 3 Insights estratégicos para investimento.
                    
                    Responda em Português de forma clara e organizada.
                    """
                    
                    response = model.generate_content(prompt)
                    
                    # Exibição do Resultado
                    st.markdown("### 📊 Relatório de Inteligência de Mercado")
                    st.write(response.text)
                    st.balloons()
                    
                except Exception as e:
                    st.error(f"Erro ao processar com a IA: {e}")
                    st.info("Dica: Se o arquivo for muito grande, tente subir uma versão com menos linhas.")

    except Exception as e:
        st.error(f"Erro ao ler o arquivo: {e}")

else:
    st.info("Aguardando upload de arquivo para começar...")
