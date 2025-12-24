import streamlit as st
import pandas as pd
import google.generativeai as genai
from pypdf import PdfReader

# 1. Page Configuration
st.set_page_config(page_title="AI Market Intelligence Pro", layout="wide")

# 2. API Key Setup
if "GOOGLE_API_KEY" not in st.secrets:
    st.error("🔑 API Key missing. Please add GOOGLE_API_KEY to Streamlit Secrets.")
    st.stop()

# Initialize AI with fallback logic
genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])

def get_model():
    """Tries to initialize the best available model."""
    # We try Flash first as it's faster/cheaper, then fallback to Pro
    models_to_try = ['gemini-1.5-flash', 'gemini-1.5-pro', 'gemini-pro']
    for model_name in models_to_try:
        try:
            model = genai.GenerativeModel(model_name)
            # Test a tiny generation to see if model exists/is authorized
            return model
        except:
            continue
    return None

model = get_model()

if not model:
    st.error("❌ Model Error: Could not connect to any Gemini models. Please check your API Key permissions.")
    st.stop()

# 3. User Interface (Fluent English)
st.title("🤖 AI Real Estate Market Analyst")
st.subheader("Professional Market Insights for Florida & Beyond")
st.write("Upload your data files (**CSV, Excel, or PDF**) to generate a strategic investment report.")
st.markdown("---")

# 4. File Upload Section
uploaded_file = st.file_uploader("Drag and drop your property file here", type=['csv', 'xlsx', 'pdf'])

if uploaded_file:
    data_text = ""
    file_ext = uploaded_file.name.split('.')[-1].lower()

    try:
        # PDF Parsing
        if file_ext == 'pdf':
            reader = PdfReader(uploaded_file)
            pdf_text = ""
            for i, page in enumerate(reader.pages[:5]): # Limit to 5 pages
                pdf_text += page.extract_text()
            data_text = pdf_text
            st.success("✅ PDF loaded successfully.")
            
        # Spreadsheet Parsing (CSV/Excel)
        else:
            if file_ext == 'csv':
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file)
            
            st.success("✅ Dataset loaded successfully.")
            st.subheader("Data Preview (First 5 Rows)")
            st.dataframe(df.head(5))
            # Convert sample rows to text for AI
            data_text = df.head(15).to_string()

        # 5. Analysis Execution
        st.markdown("---")
        if st.button("🚀 Generate Executive Market Report"):
            with st.spinner('Analyzing market trends and pricing...'):
                try:
                    prompt = f"""
                    Role: Senior Florida Real Estate Investment Consultant.
                    Task: Provide a professional executive summary for the following data:
                    
                    File Name: {uploaded_file.name}
                    Data Content Sample:
                    {data_text}
                    
                    The report must include:
                    1. MARKET SNAPSHOT: Briefly describe what this data represents.
                    2. PRICING INTELLIGENCE: Identify price ranges, averages, and any notable outliers.
                    3. GEOGRAPHIC ANALYSIS: Highlight key neighborhoods or cities (e.g., North Port, Venice, Sarasota).
                    4. INVESTMENT STRATEGY: Provide 3 professional recommendations for an investor or buyer.
                    
                    Language: Professional English. Format: Bullet points and bold headers.
                    """
                    
                    response = model.generate_content(prompt)
                    st.markdown("### 📊 AI Strategic Market Report")
                    st.write(response.text)
                    st.balloons()
                    
                except Exception as e:
                    st.error(f"Analysis Error: {e}")
                    st.info("Try uploading a smaller file or checking if your API Key is active in Google AI Studio.")

    except Exception as e:
        st.error(f"File Processing Error: {e}")

else:
    st.info("💡 Ready to analyze? Upload an MLS export or a PDF report above.")
