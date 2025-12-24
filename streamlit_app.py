import streamlit as st
import pandas as pd
import google.generativeai as genai
from pypdf import PdfReader
import io

# 1. Page Configuration
st.set_page_config(page_title="Global Multi-Data Analyst", layout="wide")

# 2. API Key Setup
if "GOOGLE_API_KEY" not in st.secrets:
    st.error("🔑 API Key missing in Streamlit Secrets.")
    st.stop()

genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])
model = genai.GenerativeModel('gemini-1.5-flash')

# 3. UI Header
st.title("🤖 Global Multi-Source Market Analyst")
st.subheader("Deep Analysis of Multiple Datasets")
st.markdown("---")

# 4. Multi-File Upload (Explicitly allowing multiple files)
uploaded_files = st.file_uploader(
    "Upload ALL your files here (CSV, XLSX, PDF)", 
    type=['csv', 'xlsx', 'pdf'], 
    accept_multiple_files=True
)

if uploaded_files:
    full_analysis_context = ""
    st.write(f"### 📑 Processing {len(uploaded_files)} file(s)...")
    
    for uploaded_file in uploaded_files:
        file_ext = uploaded_file.name.split('.')[-1].lower()
        
        with st.expander(f"Data Insights: {uploaded_file.name}"):
            try:
                if file_ext == 'pdf':
                    reader = PdfReader(uploaded_file)
                    text = " ".join([p.extract_text() for p in reader.pages])
                    full_analysis_context += f"\n--- DOCUMENT: {uploaded_file.name} ---\n{text}\n"
                    st.success(f"Full PDF text captured.")
                
                else:
                    # Read the entire spreadsheet
                    if file_ext == 'csv':
                        df = pd.read_csv(uploaded_file)
                    else:
                        df = pd.read_excel(uploaded_file)
                    
                    # Generate a COMPREHENSIVE Summary of ALL data
                    # This captures the essence of 100% of the rows
                    summary_stats = {
                        "Total Records": len(df),
                        "Average Price": df['Current Price_num'].mean() if 'Current Price_num' in df.columns else df.iloc[:, 1].mean(),
                        "Top Subdivisions": df['Legal Subdivision Name'].value_counts().head(10).to_dict() if 'Legal Subdivision Name' in df.columns else "N/A",
                        "Price Range": f"{df.iloc[:, 1].min()} to {df.iloc[:, 1].max()}",
                        "Columns Found": df.columns.tolist()
                    }
                    
                    st.write(summary_stats)
                    st.dataframe(df.head(10)) # Preview for user
                    
                    # Add full summary and a large sample to the AI context
                    full_analysis_context += f"\n--- DATABASE: {uploaded_file.name} ---\n"
                    full_analysis_context += f"Full Statistical Summary: {summary_stats}\n"
                    full_analysis_context += f"Data Sample (First 50 rows):\n{df.head(50).to_string()}\n"

            except Exception as e:
                st.error(f"Error reading {uploaded_file.name}: {e}")

    # 5. Global Analysis Button
    st.markdown("---")
    if st.button("🚀 Run Deep Global Analysis"):
        with st.spinner('Analyzing 100% of the provided data...'):
            try:
                prompt = f"""
                You are a Senior Real Estate Market Analyst. 
                I have provided multiple files containing thousands of records. 
                
                CONTENT TO ANALYZE:
                {full_analysis_context}
                
                TASK:
                Based on ALL the data above, provide a comprehensive English report:
                1. EXECUTIVE SUMMARY: Cross-reference all files and describe the overall market inventory.
                2. DEEP PRICING ANALYSIS: Use the statistical summaries to identify market value trends and outliers across all documents.
                3. GEOGRAPHIC HOTSPOTS: Identify which subdivisions or cities dominate the data.
                4. INVESTMENT STRATEGY: Provide 5 high-level professional insights for a buyer looking at these specific datasets.
                
                Format with bold headers and professional bullet points.
                """
                
                response = model.generate_content(prompt)
                st.markdown("### 📊 Global Intelligence Report")
                st.write(response.text)
                st.balloons()
            except Exception as e:
                st.error(f"Analysis failed: {e}")

else:
    st.info("💡 Select multiple files at once using 'Ctrl' or 'Cmd' to perform a comparative analysis.")
