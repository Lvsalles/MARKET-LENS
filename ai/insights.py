import pandas as pd
from typing import Dict
import google.generativeai as genai
import streamlit as st


def analyze_market(df: pd.DataFrame) -> str:
    """
    Recebe dados agregados e gera análise de mercado via Gemini.
    """

    genai.configure(api_key=st.secrets["gemini"]["api_key"])
    model = genai.GenerativeModel("gemini-1.5-pro")

    prompt = f"""
    You are a senior real estate market analyst.

    Analyze the following dataset and provide insights:

    {df.head(200).to_string(index=False)}

    Answer with:
    1. Top undervalued areas
    2. Areas with highest price growth
    3. Best investment opportunities
    4. Warning signals (overpricing, long DOM)
    """

    response = model.generate_content(prompt)
    return response.text
