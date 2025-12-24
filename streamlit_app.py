import streamlit as st
import pandas as pd
import google.generativeai as genai
from pypdf import PdfReader

# 1. Page Setup
st.set_page_config(page_title="AI Market Intelligence", layout="wide")

# 2. Security Verification
if "GOOGLE_API_KEY" not in st.secrets:
    st.error("🔑 Configuration Error: API Key missing in Streamlit Secrets.")
    st.stop()

# Initialize AI
try:
    genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])
    model = genai.GenerativeModel('gemini-1.5-flash')
except Exception as e:
    st.error(f"AI Setup Failed: {e}")

# 3. UI Header
st.title("🤖 AI Real Estate Analyst")
st.write("Professional market insights for **North Port, Venice, and Sarasota County**.")
st.markdown("---")

# 4. File Upload Section
uploaded_file = st.file_uploader("Upload your Property Data (CSV, Excel, or PDF)", type=['csv', 'xlsx', 'pdf'])

if uploaded_file:
    data_summary = ""
    file_ext = uploaded_file.name.split('.')[-1].lower()

    try:
        # PDF Parsing
        if file_ext == 'pdf':
            pdf_reader = PdfReader(uploaded_file)
            raw_text = ""
            for i, page in enumerate(pdf_reader.pages[:5]):
                raw_text += page.extract_text()
            data_summary = raw_text
            st.success("✅ PDF parsed successfully.")
            
        # Spreadsheet Parsing
        else:
            if file_ext == 'csv':
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file)
            
            st.success("✅ Dataset loaded.")
            st.subheader("Data Preview")
            st.dataframe(df.head(5))
            # Send sample rows to AI
            data_summary = df.head(15).to_string()

        # 5. Run Analysis
        st.markdown("---")
        if st.button("🚀 Generate Market Report"):
            with st.spinner('AI Analyst is reviewing the data...'):
                try:
                    prompt = f"""
                    You are a Senior Real Estate Investment Advisor in Florida.
                    Analyze the following data from the file: {uploaded_file.name}
                    
                    {data_summary}
                    
                    Provide a professional executive summary in English:
                    1. Market Snapshot: What does this data represent?
                    2. Price Analysis: Identify average prices and outliers (high/low).
                    3. Geographic Trends: Note key areas (Subdivisions/Cities).
                    4. Strategic Recommendation: 3 actionable insights for an investor.
                    """
                    
                    response = model.generate_content(prompt)
                    st.markdown("### 📊 AI Market Intelligence Report")
                    st.write(response.text)
                    st.balloons()
                    
                except Exception as e:
                    st.error(f"AI Analysis Error: {e}")

    except Exception as e:
        st.error(f"Error processing file: {e}")

else:
    st.info("💡 Ready to start? Please upload an MLS export or property report.")
