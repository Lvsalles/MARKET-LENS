import streamlit as st
import pandas as pd
import google.generativeai as genai
from pypdf import PdfReader

# 1. Page Config
st.set_page_config(page_title="Pro Real Estate Global Analyst", layout="wide")

# 2. API Key Setup
if "GOOGLE_API_KEY" not in st.secrets:
    st.error("🔑 API Key missing in Secrets.")
    st.stop()

genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])
model = genai.GenerativeModel('gemini-1.5-flash')

# ---------------------------------------------------------
# DATA NORMALIZATION LIBRARY (MAPPING)
# ---------------------------------------------------------
COLUMN_MAPPING = {
    'Current Price': 'Price', 'Current Price_num': 'Price', 'Price': 'Price',
    'Status': 'Status', 'Status_clean': 'Status',
    'Legal Subdivision Name': 'Subdivision', 'City': 'City',
    'Property Style': 'Type', 'Property Type': 'Type'
}

STATUS_MAPPING = {
    'ACT': 'Active', 'Active': 'Active',
    'SLD': 'Sold', 'Sold': 'Sold', 'Closed': 'Sold',
    'PND': 'Pending', 'Pending': 'Pending', 'Under Contract': 'Pending',
    'EXP': 'Expired', 'WDN': 'Withdrawn'
}

def normalize_dataframe(df):
    # Rename columns based on library
    df = df.rename(columns={k: v for k, v in COLUMN_MAPPING.items() if k in df.columns})
    
    # Normalize Status values
    if 'Status' in df.columns:
        df['Status'] = df['Status'].map(STATUS_MAPPING).fillna(df['Status'])
    
    # Ensure Price is numeric
    if 'Price' in df.columns:
        df['Price'] = pd.to_numeric(df['Price'].astype(str).str.replace(r'[$,]', '', regex=True), errors='coerce')
    
    return df

# ---------------------------------------------------------
# UI INTERFACE
# ---------------------------------------------------------
st.title("🤖 Global Multi-Source Market Analyst")
st.subheader("Deep Comparative Data Intelligence")
st.markdown("---")

uploaded_files = st.file_uploader("Upload files (CSV, XLSX, PDF)", type=['csv', 'xlsx', 'pdf'], accept_multiple_files=True)

if uploaded_files:
    global_summary_text = ""
    all_data_frames = []

    for uploaded_file in uploaded_files:
        file_ext = uploaded_file.name.split('.')[-1].lower()
        
        with st.expander(f"Processing: {uploaded_file.name}"):
            try:
                if file_ext == 'pdf':
                    reader = PdfReader(uploaded_file)
                    text = " ".join([p.extract_text() for p in reader.pages[:10]])
                    global_summary_text += f"\n--- PDF DOCUMENT ({uploaded_file.name}) ---\n{text[:5000]}\n"
                    st.success("PDF Content Extracted.")
                else:
                    df = pd.read_csv(uploaded_file) if file_ext == 'csv' else pd.read_excel(uploaded_file)
                    df = normalize_dataframe(df)
                    
                    # CATEGORIZATION LOGIC
                    category = "Residential"
                    if "Acreage" in str(df.columns) or "Land" in uploaded_file.name:
                        category = "Land"
                    elif "Lease" in str(df.columns) or "Rent" in str(df.columns):
                        category = "Rental"
                    
                    # CALCULATION ON 100% OF THE DATA
                    status_counts = df['Status'].value_counts().to_dict() if 'Status' in df.columns else {"Unknown": len(df)}
                    avg_price = df['Price'].mean() if 'Price' in df.columns else 0
                    
                    summary = f"""
                    FILE: {uploaded_file.name}
                    Category: {category}
                    Total Records: {len(df)}
                    Status Breakdown: {status_counts}
                    Average Price: ${avg_price:,.2f}
                    Main Subdivisions: {df['Subdivision'].value_counts().head(5).to_dict() if 'Subdivision' in df.columns else 'N/A'}
                    """
                    global_summary_text += f"\n--- DATASET SUMMARY ---\n{summary}\n"
                    st.write(summary)
                    st.dataframe(df.head(5))

            except Exception as e:
                st.error(f"Error: {e}")

    # 5. GLOBAL ANALYSIS
    st.markdown("---")
    if st.button("🚀 Run Deep Comparative Analysis"):
        with st.spinner('AI is processing 100% of your data using the Library...'):
            prompt = f"""
            You are a Senior Investment Consultant.
            I have normalized multiple real estate files. Here is the aggregate data summary:
            
            {global_summary_text}
            
            INSTRUCTIONS:
            1. Clearly separate the analysis by STATUS (Active vs Sold vs Pending).
            2. For each status, report the total count of properties.
            3. Compare the categories: Residential Sales, Land, and Rentals.
            4. Identify geographic hotspots based on the Subdivision data.
            5. Provide 5 Strategic Insights for an investor.
            
            Language: Fluent Professional English. Format: Professional headers and bullet points.
            """
            
            try:
                # Fallback model selection to avoid 404
                available_models = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
                target_model = 'models/gemini-1.5-flash' if 'models/gemini-1.5-flash' in available_models else available_models[0]
                
                final_model = genai.GenerativeModel(target_model)
                response = final_model.generate_content(prompt)
                st.markdown("### 📊 Global Strategic Intelligence Report")
                st.write(response.text)
                st.balloons()
            except Exception as e:
                st.error(f"Analysis failed: {e}")

else:
    st.info("💡 Pro Tip: Select multiple files at once to see the comparative report by category and status.")
