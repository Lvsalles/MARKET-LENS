import streamlit as st
import pandas as pd
import google.generativeai as genai
from pypdf import PdfReader
import numpy as np

# 1. Page Configuration
st.set_page_config(page_title="AI Market Intelligence Pro", layout="wide")

# 2. API Key Security Check
if "GOOGLE_API_KEY" not in st.secrets:
    st.error("🔑 API Key missing. Please add GOOGLE_API_KEY to Streamlit Secrets.")
    st.stop()

genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])

# ---------------------------------------------------------
# STANDARDIZATION LIBRARY (The System's Brain)
# ---------------------------------------------------------
STANDARD_LIBRARY = {
    'columns': {
        'Current Price': 'Price', 'Current Price_num': 'Price', 'List Price': 'Price', 'Sold Price': 'Price',
        'Legal Subdivision Name': 'Subdivision', 'Subdivision/Condo Name': 'Subdivision',
        'Heated Area': 'SqFt', 'Heated Area_num': 'SqFt', 'Living Area': 'SqFt',
        'CDOM': 'DOM', 'ADOM': 'DOM', 'Days to Contract': 'DOM', 'Days to Contract_num': 'DOM',
        'Status_clean': 'Status', 'LSC List Side': 'Status',
        'Total Acreage_num': 'LotSize', 'Lot Size Square Footage_num': 'LotSize'
    },
    'status_values': {
        'ACT': 'Active', 'Active': 'Active', 'A': 'Active',
        'SLD': 'Sold', 'Sold': 'Sold', 'S': 'Sold', 'Closed': 'Sold',
        'PND': 'Pending', 'Pending': 'Pending', 'P': 'Pending', 'Under Contract': 'Pending'
    }
}

def process_and_standardize(uploaded_file):
    name = uploaded_file.name.lower()
    ext = name.split('.')[-1]
    
    if ext == 'pdf':
        reader = PdfReader(uploaded_file)
        return " ".join([p.extract_text() for p in reader.pages[:10]]), "Document"
    
    # Read Spreadsheet
    df = pd.read_csv(uploaded_file) if ext == 'csv' else pd.read_excel(uploaded_file)
    
    # 1. Rename columns using the library
    df = df.rename(columns={k: v for k, v in STANDARD_LIBRARY['columns'].items() if k in df.columns})
    
    # 2. Handle duplicates (Keep the last one, usually the _num version)
    df = df.loc[:, ~df.columns.duplicated(keep='last')]
    
    # 3. Normalize Status Values
    if 'Status' in df.columns:
        df['Status'] = df['Status'].map(STANDARD_LIBRARY['status_values']).fillna(df['Status'])
    
    # 4. Clean Numeric Columns
    for col in ['Price', 'SqFt', 'DOM', 'LotSize']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(r'[$,]', '', regex=True), errors='coerce')

    # 5. Categorize File
    category = "Residential"
    if "land" in name or "lots" in name or 'LotSize' in df.columns: category = "Land"
    elif "rent" in name or "lease" in name: category = "Rental"
    
    return df, category

# ---------------------------------------------------------
# UI INTERFACE
# ---------------------------------------------------------
st.title("🤖 AI Real Estate Intelligence Station")
st.subheader("Data-Driven Analysis for North Port, Venice & Beyond")
st.markdown("---")

files = st.file_uploader("Upload MLS files (Residential, Land, Rentals)", type=['csv', 'xlsx', 'pdf'], accept_multiple_files=True)

if files:
    global_intelligence_data = ""
    st.write("### 📑 Processing Data Layers...")
    
    for f in files:
        with st.expander(f"Analyzing: {f.name}"):
            data, category = process_and_standardize(f)
            
            if isinstance(data, pd.DataFrame):
                # Calculate Detailed Stats for 100% of the rows
                stats_summary = f"\nFILE: {f.name} | CATEGORY: {category}\n"
                
                if 'Status' in data.columns:
                    for status in data['Status'].unique():
                        subset = data[data['Status'] == status]
                        avg_p = subset['Price'].mean() if 'Price' in subset.columns else 0
                        top_sub = subset['Subdivision'].value_counts().head(5).to_dict() if 'Subdivision' in subset.columns else "N/A"
                        
                        stats_summary += f"- Status: {status} | Count: {len(subset)} | Avg Price: ${avg_p:,.2f} | Hotspots: {top_sub}\n"
                    
                    st.success(f"Category: {category}")
                    st.write(data['Status'].value_counts())
                else:
                    stats_summary += "- General Summary: " + str(data.describe(include=[np.number]).to_dict()) + "\n"
                
                global_intelligence_data += stats_summary
                st.dataframe(df_preview := data.head(5))
            else:
                global_intelligence_data += f"\nFILE: {f.name} (PDF Info):\n{data[:3000]}\n"
                st.info("PDF Content Extracted.")

    # 6. GLOBAL ANALYSIS (AI POWERED)
    st.markdown("---")
    if st.button("🚀 Generate Strategic Intelligence Report"):
        with st.spinner('The AI is thinking...'):
            try:
                # Prompt that lets the AI think freely
                prompt = f"""
                You are a Master Real Estate Investment Strategist in Florida.
                I am providing you with a high-level data summary normalized from multiple MLS sources.
                
                YOUR DATA SOURCE:
                {global_intelligence_data}
                
                YOUR MISSION:
                Think critically about these numbers. Don't just list them. 
                Interpret the relationship between Active inventory and Sold velocity. 
                Identify if there is an oversupply in specific categories (like Land) or a shortage in others.
                Look for pricing gaps between Residential Sales and Rental yields.
                
                REPORT STRUCTURE (Fluent English):
                1. THE BOTTOM LINE: A 2-sentence executive summary of the current market state.
                2. TRANSACTIONAL VELOCITY: Analyze Active vs Sold per category.
                3. GEOGRAPHIC POWER ZONES: Where is the money moving based on Subdivision hotspots?
                4. INVESTOR'S EDGE: 5 highly strategic, non-obvious recommendations.
                
                Be professional, sharp, and data-driven.
                """
                
                response = genai.GenerativeModel('gemini-1.5-flash').generate_content(prompt)
                st.markdown("### 📊 AI Strategic Intelligence Report")
                st.write(response.text)
                st.balloons()
            except Exception as e:
                st.error(f"Analysis Failed: {e}")
else:
    st.info("💡 Pro Tip: Upload all files at once (Cmd+Click) to see the comparative analysis.")
