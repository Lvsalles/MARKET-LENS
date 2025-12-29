import google.generativeai as genai
import streamlit as st
import pandas as pd


def get_gemini_model():
    if "gemini" not in st.secrets or "api_key" not in st.secrets["gemini"]:
        raise RuntimeError("Gemini API key não encontrada nos secrets.")

    genai.configure(api_key=st.secrets["gemini"]["api_key"])
    return genai.GenerativeModel("gemini-1.5-pro")


def analyze_market(df: pd.DataFrame) -> str:
    model = get_gemini_model()

    prompt = f"""
Você é um analista imobiliário sênior.

Analise os dados abaixo e responda de forma objetiva:

1. Quais regiões estão subavaliadas?
2. Onde existe maior potencial de valorização?
3. Quais métricas indicam risco?
4. O que um investidor deveria fazer agora?

DADOS:
{df.head(200).to_string(index=False)}
"""

    response = model.generate_content(prompt)
    return response.text
