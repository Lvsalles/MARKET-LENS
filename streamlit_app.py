import streamlit as st
import pandas as pd
import google.generativeai as genai
from pypdf import PdfReader

# 1. Page Configuration
st.set_page_config(page_title="AI Market Intelligence Pro", layout="wide")

# 2. API Key Check
if "GOOGLE_API_KEY" not in st.secrets:
    st.error("🔑 Configuration Error: GOOGLE_API_KEY not found in Streamlit Secrets.")
    st.stop()

# Initialize API
genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])

def initialize_model():
    """Attempt to load the best available Gemini model."""
    # List of models to try in order of preference
    model_names = ['gemini-1.5-flash', 'gemini-1.5-pro', 'gemini-pro']
    
    for name in model_names:
        try:
            m = genai.GenerativeModel(name)
            # Simple test to check if the model is reachable
            return m, name
        except Exception:
            continue
    return None, None

model, active_model_name = initialize_model()

if not model:
    st.error("❌ Model Error: Could not connect to Google AI. Please verify your API Key and internet connection.")
    st.stop()

# 3. Fluent English UI
st.title("🤖 AI Real Estate Market Analyst")
st.subheader("Advanced Data Intelligence for Real Estate Professionals")
st.write(f"Active Engine: `{active_model_name}`")
st.markdown("---")

# 4. File Upload Section
uploaded_file = st.file_uploader("Upload your Property Data (CSV, Excel, or PDF)", type=['csv', 'xlsx', 'pdf'])

if uploaded_file:
    data_context = ""
    file_type = uploaded_file.name.split('.')[-1].lower()

    try:
        # PDF Logic
        if file_type == 'pdf':
            reader = PdfReader(uploaded_file)
            pdf_text = ""
            for i, page in enumerate(reader.pages[:5]): # Scan first 5 pages
                pdf_text += page.extract_text()
            data_context = pdf_text
            st.success("✅ PDF Analysis Ready.")
            
        # Spreadsheet Logic
        else:
            if file_type == 'csv':
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file)
            
            st.success("✅ Dataset Loaded.")
            st.subheader("Market Data Preview")
            st.dataframe(df.head(5))
            # Feed the first 15 rows to the AI for a snapshot
            data_context = df.head(15).to_string()

        # 5. The Analysis Engine
        st.markdown("---")
        if st.button("🚀 Generate Executive Market Report"):
            with st.spinner('AI is analyzing market trends and property values...'):
                try:
                    # Professional Prompt for US Market
                    prompt = f"""
                    Role: Senior Florida Real Estate Investment Consultant.
                    Source File: {uploaded_file.name}
                    
                    Task: Provide a high-level executive summary in English based on the data below:
                    
                    {data_context}
                    
                    Required Sections:
                    1. MARKET SUMMARY: Define what this data covers (e.g., North Port listings, Venice sales).
                    2. PRICING INTELLIGENCE: Analyze price ranges, averages, and significant outliers.
                    3. GEOGRAPHIC TRENDS: Highlight performance by Subdivisions or Zip Codes.
                    4. STRATEGIC ADVICE: Provide 3 actionable investment insights for a professional buyer.
                    
                    Format: Use bold headers and professional bullet points.
                    """
                    
                    response = model.generate_content(prompt)
                    
                    st.markdown("### 📊 AI Strategic Intelligence Report")
                    st.write(response.text)
                    st.balloons()
                    
                except Exception as e:
                    st.error(f"Analysis Failed: {e}")
                    st.info("Tip: This can happen if the data format is unusual. Try a standard MLS CSV export.")

    except Exception as e:
        st.error(f"File Error: {e}")

else:
    st.info("💡 Ready to start? Drag your MLS export or property PDF here.")
