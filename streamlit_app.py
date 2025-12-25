import streamlit as st
import pandas as pd
import google.generativeai as genai
from pypdf import PdfReader
import io

# 1. Page Configuration
st.set_page_config(page_title="AI Market Intelligence Analyst", layout="wide")

# 2. API Key Security Check
if "GOOGLE_API_KEY" not in st.secrets:
    st.error("🔑 API Key not found. Please add GOOGLE_API_KEY to Streamlit Secrets.")
    st.stop()

genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])

# 3. Data Intelligence Library (Normalization)
# This handles the different naming conventions across your MLS files
SYNONYMS = {
    'columns': {
        'Current Price': 'Price', 'Current Price_num': 'Price', 'List Price': 'Price',
        'Legal Subdivision Name': 'Subdivision', 'Subdivision/Condo Name': 'Subdivision',
        'Heated Area': 'SqFt', 'Heated Area_num': 'SqFt',
        'CDOM': 'DOM', 'ADOM': 'DOM', 'Days to Contract': 'DOM'
    },
    'status': {
        'ACT': 'Active', 'Active': 'Active', 'A': 'Active',
        'SLD': 'Sold', 'Sold': 'Sold', 'S': 'Sold', 'Closed': 'Sold',
        'PND': 'Pending', 'Pending': 'Pending', 'P': 'Pending', 'Under Contract': 'Pending'
    }
}

def analyze_dataset(file):
    name = file.name.lower()
    ext = name.split('.')[-1]
    
    if ext == 'pdf':
        reader = PdfReader(file)
        return " ".join([p.extract_text() for p in reader.pages[:10]]), "Document"
    
    # Read Data
    df = pd.read_csv(file) if ext == 'csv' else pd.read_excel(file)
    
    # Normalize Columns and Status
    df = df.rename(columns={k: v for k, v in SYNONYMS['columns'].items() if k in df.columns})
    if 'Status' in df.columns:
        df['Status'] = df['Status'].map(SYNONYMS['status']).fillna(df['Status'])
    
    # Clean Price Column
    if 'Price' in df.columns:
        df['Price'] = pd.to_numeric(df['Price'].astype(str).str.replace(r'[$,]', '', regex=True), errors='coerce')

    # Automatic Categorization
    category = "Residential"
    if "land" in name or "acreage_num" in df.columns or "Total Acreage" in df.columns:
        category = "Land"
    elif "rental" in name or "lease" in str(df.columns).lower() or "rent" in name:
        category = "Rental"
    
    return df, category

# 4. User Interface
st.title("🤖 Global Market Intelligence Analyst")
st.subheader("Deep Multi-Source Portfolio Analysis (North Port & Venice)")
st.markdown("---")

uploaded_files = st.file_uploader("Upload all MLS files (Residential, Land, Rentals)", type=['csv', 'xlsx', 'pdf'], accept_multiple_files=True)

if uploaded_files:
    aggregated_stats = ""
    st.write(f"### 📊 Processing {len(uploaded_files)} Datasets...")
    
    for f in uploaded_files:
        with st.expander(f"Inspecting: {f.name}"):
            data, category = analyze_dataset(f)
            
            if isinstance(data, pd.DataFrame):
                # We calculate stats for 100% of the data in the file
                file_report = f"\nFILE: {f.name} | CATEGORY: {category}\n"
                
                if 'Status' in data.columns:
                    for status in data['Status'].unique():
                        subset = data[data['Status'] == status]
                        avg_price = subset['Price'].mean() if 'Price' in subset.columns else 0
                        top_subs = subset['Subdivision'].value_counts().head(5).to_dict() if 'Subdivision' in subset.columns else {}
                        
                        summary = f"- Status: {status} | Count: {len(subset)} | Avg Price: ${avg_price:,.2f} | Top Subdivisions: {top_subs}\n"
                        file_report += summary
                    
                    st.success(f"Categorized as: {category}")
                    st.write(data['Status'].value_counts())
                else:
                    file_report += "- No status column found.\n"
                
                aggregated_stats += file_report
            else:
                aggregated_stats += f"\nFILE: {f.name} (PDF Document):\n{data[:3000]}\n"
                st.info("PDF Content Captured.")

    # 5. Global Analysis
    st.markdown("---")
    if st.button("🚀 Run Comprehensive Market Analysis"):
        with st.spinner('Generating Executive Report...'):
            try:
                # Find best available model
                available = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
                target_model = 'models/gemini-1.5-flash' if 'models/gemini-1.5-flash' in available else available[0]
                model = genai.GenerativeModel(target_model)
                
                prompt = f"""
                You are a Senior Investment Consultant. Analyze the REAL market data summary below.
                IMPORTANT: Do NOT use simulated or placeholder data. Use the exact counts and averages provided.

                MASTER DATA SUMMARY:
                {aggregated_stats}

                REPORT REQUIREMENTS (Fluent English):
                1. GLOBAL SNAPSHOT: Group ALL files by STATUS (Active vs Sold vs Pending) and report the grand total counts.
                2. CATEGORY COMPARISON: Compare dynamics between Residential Sales, Land parcels, and Rentals. 
                3. GEOGRAPHIC HOTSPOTS: Identify the top Subdivisions where most SOLD activity is happening.
                4. STRATEGIC INVESTOR INSIGHTS: Provide 5 professional, high-level recommendations.

                Format: Use bold headers, professional bullet points, and a formal tone.
                """
                
                response = model.generate_content(prompt)
                st.markdown("### 📊 Global Market Intelligence Executive Report")
                st.write(response.text)
                st.balloons()
            except Exception as e:
                st.error(f"Analysis Failed: {e}")
else:
    st.info("💡 Please upload multiple files to start the comparative analysis.")
