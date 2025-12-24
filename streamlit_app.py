import streamlit as st
import pandas as pd
import google.generativeai as genai
from pypdf import PdfReader

# 1. Page Config
st.set_page_config(page_title="Global Real Estate Intelligence", layout="wide")

# 2. API Key Setup
if "GOOGLE_API_KEY" not in st.secrets:
    st.error("🔑 API Key missing in Streamlit Secrets.")
    st.stop()

genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])
model = genai.GenerativeModel('gemini-1.5-flash')

# ---------------------------------------------------------
# DATA NORMALIZATION LIBRARY (Synonyms Mapping)
# ---------------------------------------------------------
COLUMN_MAP = {
    'Current Price': 'Price', 'Current Price_num': 'Price', 'List Price': 'Price',
    'Status': 'Status', 'Status_clean': 'Status', 'LSC List Side': 'Status',
    'Legal Subdivision Name': 'Subdivision', 'Subdivision/Condo Name': 'Subdivision',
    'City': 'City', 'Zip': 'Zip', 'ML Number': 'ML_Number'
}

STATUS_MAP = {
    'ACT': 'Active', 'Active': 'Active', 'A': 'Active',
    'SLD': 'Sold', 'Sold': 'Sold', 'S': 'Sold', 'Closed': 'Sold',
    'PND': 'Pending', 'Pending': 'Pending', 'P': 'Pending', 'Under Contract': 'Pending'
}

def clean_and_normalize(df, filename):
    # 1. Rename columns based on Library
    df = df.rename(columns={k: v for k, v in COLUMN_MAP.items() if k in df.columns})
    
    # 2. Normalize Status
    if 'Status' in df.columns:
        df['Status'] = df['Status'].map(STATUS_MAP).fillna(df['Status'])
    
    # 3. Handle Prices (Clean strings like $40,000)
    if 'Price' in df.columns:
        df['Price'] = pd.to_numeric(df['Price'].astype(str).str.replace(r'[$,]', '', regex=True), errors='coerce')

    # 4. Categorize File Type
    category = "Residential"
    if "land" in filename.lower() or "acreage" in str(df.columns).lower():
        category = "Land"
    elif "lease" in str(df.columns).lower() or "rent" in str(df.columns).lower():
        category = "Rental"
    
    return df, category

# ---------------------------------------------------------
# UI INTERFACE (English)
# ---------------------------------------------------------
st.title("🤖 Global Multi-Source Market Analyst")
st.subheader("Professional Comparative Intelligence - North Port & Region")
st.markdown("---")

uploaded_files = st.file_uploader("Upload all files (CSV, XLSX, PDF)", type=['csv', 'xlsx', 'pdf'], accept_multiple_files=True)

if uploaded_files:
    combined_summary = ""
    st.write(f"### 📑 Data Normalization in Progress...")

    for uploaded_file in uploaded_files:
        file_ext = uploaded_file.name.split('.')[-1].lower()
        
        with st.expander(f"Reviewing: {uploaded_file.name}"):
            try:
                if file_ext == 'pdf':
                    reader = PdfReader(uploaded_file)
                    text = " ".join([p.extract_text() for p in reader.pages[:5]])
                    combined_summary += f"\n[FILE TYPE: PDF | NAME: {uploaded_file.name}]\n{text[:3000]}\n"
                    st.success("PDF Text Extracted.")
                else:
                    raw_df = pd.read_csv(uploaded_file) if file_ext == 'csv' else pd.read_excel(uploaded_file)
                    df, cat = clean_and_normalize(raw_df, uploaded_file.name)
                    
                    # AGGREGATE STATS BY STATUS
                    if 'Status' in df.columns:
                        stats = []
                        for status in df['Status'].unique():
                            subset = df[df['Status'] == status]
                            subdivs = subset['Subdivision'].value_counts().head(5).to_dict() if 'Subdivision' in subset.columns else "N/A"
                            avg_p = subset['Price'].mean()
                            stats.append(f"Status: {status} | Count: {len(subset)} | Avg Price: ${avg_p:,.2f} | Top Subdivisions: {subdivs}")
                        
                        summary_report = "\n".join(stats)
                        combined_summary += f"\n[CATEGORY: {cat} | FILE: {uploaded_file.name}]\n{summary_report}\n"
                        st.write(f"Category: **{cat}**")
                        st.write(status_counts := df['Status'].value_counts())
                        st.dataframe(df.head(5))
                    else:
                        combined_summary += f"\n[FILE: {uploaded_file.name}] - No status column found.\n"

            except Exception as e:
                st.error(f"Error processing {uploaded_file.name}: {e}")

    # 5. GENERATE REPORT
    st.markdown("---")
    if st.button("🚀 Run Global Executive Analysis"):
        with st.spinner('AI
