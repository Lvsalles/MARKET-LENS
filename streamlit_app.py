import streamlit as st
import pandas as pd
import google.generativeai as genai
from pypdf import PdfReader

# 1. Page Configuration
st.set_page_config(page_title="Global Multi-Source Analyst", layout="wide")

# 2. API Key Setup
if "GOOGLE_API_KEY" not in st.secrets:
    st.error("🔑 API Key missing. Please add GOOGLE_API_KEY to Streamlit Secrets.")
    st.stop()

genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])

# 3. Data Intelligence Library (Synonyms Mapping)
# This library ensures that different names across files are treated as the same thing
COLUMN_MAPPING = {
    'Current Price': 'Price', 'Current Price_num': 'Price', 'List Price': 'Price',
    'Status': 'Status', 'Status_clean': 'Status', 'LSC List Side': 'Status',
    'Legal Subdivision Name': 'Subdivision', 'Subdivision/Condo Name': 'Subdivision',
    'City': 'City', 'Zip': 'Zip', 'ML Number': 'ML_Number'
}

STATUS_MAPPING = {
    'ACT': 'Active', 'Active': 'Active', 'A': 'Active',
    'SLD': 'Sold', 'Sold': 'Sold', 'S': 'Sold', 'Closed': 'Sold',
    'PND': 'Pending', 'Pending': 'Pending', 'P': 'Pending', 'Under Contract': 'Pending'
}

def normalize_data(df, filename):
    # Rename columns based on library
    df = df.rename(columns={k: v for k, v in COLUMN_MAPPING.items() if k in df.columns})
    # Normalize Status values
    if 'Status' in df.columns:
        df['Status'] = df['Status'].map(STATUS_MAPPING).fillna(df['Status'])
    # Clean Price data
    if 'Price' in df.columns:
        df['Price'] = pd.to_numeric(df['Price'].astype(str).str.replace(r'[$,]', '', regex=True), errors='coerce')
    
    # Identify Category
    category = "Residential"
    if "land" in filename.lower() or "acreage" in str(df.columns).lower():
        category = "Land"
    elif "lease" in str(df.columns).lower() or "rent" in str(df.columns).lower():
        category = "Rental"
    return df, category

# 4. UI Header
st.title("🤖 Global Multi-Source Market Analyst")
st.subheader("Professional Comparative Intelligence - Fluent English")
st.markdown("---")

uploaded_files = st.file_uploader("Upload all your files (CSV, XLSX, PDF)", type=['csv', 'xlsx', 'pdf'], accept_multiple_files=True)

if uploaded_files:
    combined_context = ""
    st.write(f"### 📑 Processing {len(uploaded_files)} files...")

    for uploaded_file in uploaded_files:
        file_ext = uploaded_file.name.split('.')[-1].lower()
        with st.expander(f"Reviewing: {uploaded_file.name}"):
            try:
                if file_ext == 'pdf':
                    reader = PdfReader(uploaded_file)
                    text = " ".join([p.extract_text() for p in reader.pages[:10]])
                    combined_context += f"\n[FILE: {uploaded_file.name} | TYPE: PDF]\n{text[:3000]}\n"
                    st.success("PDF parsed.")
                else:
                    raw_df = pd.read_csv(uploaded_file) if file_ext == 'csv' else pd.read_excel(uploaded_file)
                    df, category = normalize_data(raw_df, uploaded_file.name)
                    
                    # Calculate Global Stats across 100% of rows
                    if 'Status' in df.columns:
                        stats_by_status = ""
                        for status in df['Status'].unique():
                            subset = df[df['Status'] == status]
                            avg_p = subset['Price'].mean() if 'Price' in subset.columns else 0
                            top_sub = subset['Subdivision'].value_counts().head(5).to_dict() if 'Subdivision' in subset.columns else "N/A"
                            stats_by_status += f"Status: {status} | Count: {len(subset)} | Avg Price: ${avg_p:,.2f} | Subdivisions: {top_sub}\n"
                        
                        combined_context += f"\n[CATEGORY: {category} | FILE: {uploaded_file.name}]\n{stats_by_status}\n"
                        st.write(f"Category: **{category}**")
                        st.write(df['Status'].value_counts())
                    else:
                        combined_context += f"\n[FILE: {uploaded_file.name}] - No Status column found.\n"
                    st.dataframe(df.head(5))
            except Exception as e:
                st.error(f"Error: {e}")

    # 5. Global Analysis Execution
    st.markdown("---")
    if st.button("🚀 Run Deep Comparative Analysis"):
        with st.spinner('AI is generating the Strategic Report...'):
            try:
                # Get the best model
                avail_models = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
                model_name = 'models/gemini-1.5-flash' if 'models/gemini-1.5-flash' in avail_models else avail_models[0]
                model = genai.GenerativeModel(model_name)
                
                prompt = f"""
                You are a Senior Investment Consultant. Analyze the normalized data below.
                
                AGGREGATED DATA SUMMARY:
                {combined_context}
                
                REQUIREMENTS (Fluent English):
                1. GLOBAL SNAPSHOT: Group data by STATUS (Active vs Sold vs Pending) and report total counts for each.
                2. CATEGORY COMPARISON: Compare Residential vs Land vs Rental market dynamics.
                3. GEOGRAPHIC HOTSPOTS: Use Subdivision data to identify where activity is highest.
                4. INVESTOR INSIGHTS: Provide 5 high-level strategic recommendations.
                
                Format with professional headers and bullet points.
                """
                response = model.generate_content(prompt)
                st.markdown("### 📊 Global Strategic Intelligence Report")
                st.write(response.text)
                st.balloons()
            except Exception as e:
                st.error(f"Analysis Error: {e}")
else:
    st.info("💡 Please upload multiple files to begin.")
