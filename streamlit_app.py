import streamlit as st
import pandas as pd
import google.generativeai as genai
from pypdf import PdfReader

# 1. Page Configuration
st.set_page_config(page_title="AI Real Estate Analyst", layout="wide")

# 2. API Key Security Check
if "GOOGLE_API_KEY" not in st.secrets:
    st.error("🔑 Error: API Key not found in Streamlit Secrets.")
    st.stop()

# Configure Google Gemini
try:
    genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])
    model = genai.GenerativeModel('gemini-1.5-flash')
except Exception as e:
    st.error(f"AI Configuration Error: {e}")

# UI Header
st.title("🤖 AI Real Estate Market Analyst")
st.write("Upload your **CSV, Excel, or PDF** files for instant market insights.")
st.markdown("---")

# 3. File Upload Section
uploaded_file = st.file_uploader("Drop your property data file here", type=['csv', 'xlsx', 'pdf'])

if uploaded_file:
    content_to_analyze = ""
    file_extension = uploaded_file.name.split('.')[-1].lower()

    try:
        # PDF Processing
        if file_extension == 'pdf':
            reader = PdfReader(uploaded_file)
            pdf_text = ""
            # Limit to first 5 pages for stability
            for i, page in enumerate(reader.pages[:5]):
                pdf_text += page.extract_text()
            content_to_analyze = pdf_text
            st.success("✅ PDF loaded successfully!")
            st.info("Document Preview:")
            st.text(content_to_analyze[:400] + "...")

        # Spreadsheet Processing (CSV/XLSX)
        else:
            if file_extension == 'csv':
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file)
            
            st.success("✅ Spreadsheet loaded successfully!")
            st.subheader("Data Preview (Top 5 Rows)")
            st.dataframe(df.head(5))
            
            # Send the first 15 rows to the AI to avoid size errors
            content_to_analyze = df.head(15).to_string()

        # 4. AI Analysis Execution
        st.markdown("---")
        if st.button("🚀 Run AI Market Analysis"):
            with st.spinner('AI is processing data and generating professional insights...'):
                try:
                    # Professional Real Estate Prompt
                    prompt = f"""
                    You are a Senior Real Estate Investment Analyst in Florida.
                    Analyze the following data from the file: {uploaded_file.name}
                    
                    Data Content:
                    {content_to_analyze}
                    
                    Please provide a professional report in English including:
                    1. Data Summary: Identify if this is a listing list, sales report, or land data.
                    2. Pricing Analysis: Average price, highest/lowest values, and price per sqft if available.
                    3. Location Highlights: Top subdivisions or cities (e.g., North Port, Venice).
                    4. Investment Strategy: 3 professional insights for a buyer or investor.
                    
                    Format the output with clear headings and bullet points.
                    """
                    
                    response = model.generate_content(prompt)
                    
                    # Display Results
                    st.markdown("### 📊 AI Market Intelligence Report")
                    st.write(response.text)
                    st.balloons()
                    
                except Exception as e:
                    st.error(f"AI Processing Error: {e}")
                    st.info("Tip: If the file is too large, try uploading a smaller version.")

    except Exception as e:
        st.error(f"File Reading Error: {e}")

else:
    st.info("💡 Pro Tip: Upload your MLS or Land export file to get started.")
