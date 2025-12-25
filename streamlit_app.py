import streamlit as st
import pandas as pd
import google.generativeai as genai
from pypdf import PdfReader
from docx import Document
import folium
from streamlit_folium import folium_static
import numpy as np
import re

# 1. Page Configuration
st.set_page_config(page_title="AI Investor Intelligence Hub", layout="wide")

# 2. AI & API Setup
if "GOOGLE_API_KEY" not in st.secrets:
    st.error("🔑 API Key Missing.")
    st.stop()

genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])

# ---------------------------------------------------------
# UNIVERSAL STANDARDIZATION & MAPPING LIBRARY
# ---------------------------------------------------------
COLUMN_MAP = {
    'Price': ['Current Price', 'Current Price_num', 'List Price', 'Sold Price', 'Price'],
    'Status': ['Status', 'Listing Status', 'LSC List Side'],
    'Address': ['Address', 'Full Address', 'Street Address'],
    'City': ['City'], 'Zip': ['Zip', 'Zip Code'],
    'Subdivision': ['Legal Subdivision Name', 'Subdivision/Condo Name', 'Subdivision'],
    'Beds': ['Beds', 'Beds_num', 'Bedrooms'],
    'Baths': ['Full Baths', 'Full Baths_num', 'Bathrooms'],
    'SqFt': ['Heated Area', 'Heated Area_num', 'SqFt', 'Living Area'],
    'Year': ['Year Built', 'Year Built_num'],
    'Garage': ['Garage', 'Garage Spaces', 'Carport'],
    'Pool': ['Pool', 'Pool Private', 'Pool Features'],
    'DOM': ['CDOM', 'ADOM', 'Days to Contract', 'DOM'],
    'Agent': ['List Agent', 'Listing Agent', 'Agent Name'],
    'Financing': ['Sold Terms', 'Terms', 'Financing'],
    'Zoning': ['Zoning', 'Zoning Code', 'Land Use']
}

STATUS_MAP = {
    'ACT': 'Active', 'SLD': 'Sold', 'PND': 'Pending', 'Closed': 'Sold', 'Active': 'Active'
}

def normalize_dataset(df):
    # Rename columns based on map
    for std_col, syns in COLUMN_MAP.items():
        found = next((c for c in df.columns if c in syns), None)
        if found:
            df = df.rename(columns={found: std_col})
    
    # Keep only standardized columns + originals for AI context
    if 'Status' in df.columns:
        df['Status'] = df['Status'].map(STATUS_MAPPING).fillna(df['Status'])
    
    # Numeric conversions
    for col in ['Price', 'SqFt', 'DOM', 'Beds', 'Baths', 'Year']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(r'[$,]', '', regex=True), errors='coerce')
    
    # Calculate Price per SqFt
    if 'Price' in df.columns and 'SqFt' in df.columns:
        df['PPSqFt'] = df['Price'] / df['SqFt']
        
    return df

# ---------------------------------------------------------
# UI & NAVIGATION
# ---------------------------------------------------------
st.sidebar.title("🚀 Analysis Terminal")
analysis_mode = st.sidebar.selectbox(
    "Select Intelligence Level",
    ["Market Macro View", "Zipcode & Street Deep-Dive", "Investment Scorecard (Arbitrage)", "Agent & Closing Performance"]
)

st.title("🏙️ AI Real Estate Investment Hub")
st.write(f"Active Analysis: **{analysis_mode}**")
st.markdown("---")

files = st.file_uploader("Upload MLS/Land/Rental Datasets", type=['csv', 'xlsx', 'pdf', 'docx'], accept_multiple_files=True)

if files:
    full_db = []
    text_context = ""
    
    for f in files:
        ext = f.name.split('.')[-1].lower()
        if ext in ['csv', 'xlsx']:
            df_raw = pd.read_csv(f) if ext == 'csv' else pd.read_excel(f)
            df_norm = normalize_dataset(df_raw)
            # Tag category
            cat = "Land" if "land" in f.name.lower() else "Residential"
            if "rent" in f.name.lower(): cat = "Rental"
            df_norm['Category'] = cat
            full_db.append(df_norm)
        elif ext == 'pdf':
            reader = PdfReader(f)
            text_context += f"\n[PDF: {f.name}]\n" + " ".join([p.extract_text() for p in reader.pages[:5]])

    if full_db:
        main_df = pd.concat(full_db, ignore_index=True)
        
        # DASHBOARD METRICS
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Listings", len(main_df))
        if 'Price' in main_df.columns:
            col2.metric("Avg Price", f"${main_df['Price'].mean():,.0f}")
        if 'PPSqFt' in main_df.columns:
            col3.metric("Avg $/SqFt", f"${main_df['PPSqFt'].mean():,.2f}")
        if 'Status' in main_df.columns:
            col4.metric("Sold Ratio", f"{(len(main_df[main_df['Status']=='Sold'])/len(main_df)*100):.1f}%")

        # MAP SECTION
        st.subheader("📍 Geospatial Distribution")
        m = folium.Map(location=[27.05, -82.25], zoom_start=11)
        # (Simplified markers for brevity)
        for _, row in main_df.dropna(subset=['Address']).head(50).iterrows():
            folium.Marker(
                [27.05 + np.random.uniform(-0.05, 0.05), -82.25 + np.random.uniform(-0.05, 0.05)],
                popup=f"{row['Address']} - ${row.get('Price', 0):,.0f}",
                tooltip="Click for G-Maps Link"
            ).add_to(m)
        folium_static(m)

        # ---------------------------------------------------------
        # AI STRATEGIC REPORT GENERATION
        # ---------------------------------------------------------
        if st.button("🚀 Generate High-Level Strategic Report"):
            with st.spinner("AI analyzing micro-segments and zoning patterns..."):
                # Data payload for AI
                stats_payload = {
                    "by_zip": main_df.groupby('Zip')['Price'].mean().to_dict() if 'Zip' in main_df.columns else {},
                    "by_type": main_df.groupby('Category')['Price'].mean().to_dict(),
                    "top_agents": main_df['Agent'].value_counts().head(5).to_dict() if 'Agent' in main_df.columns else {},
                    "financing": main_df['Financing'].value_counts().to_dict() if 'Financing' in main_df.columns else {}
                }

                prompt = f"""
                You are a Real Estate Investment Specialist. Analyze the provided data:
                
                STATS: {stats_payload}
                DOCUMENT CONTEXT: {text_context[:2000]}
                
                TASK:
                Perform a "{analysis_mode}" analysis. 
                1. Identify the 'Alpha' (where the best profit lies).
                2. Cross-reference financing (Cash vs Conv) with speed of sale.
                3. Mention Zoning/ADU potential for subdivisions appearing in the data (North Port/Venice focus).
                4. Highlight underpriced Zipcodes based on PPSqFt.
                5. Provide 5 investment plays (strategies).

                Output in Professional English with Google Maps links placeholders for hotspots.
                """
                
                model = genai.GenerativeModel('gemini-1.5-flash')
                response = model.generate_content(prompt)
                st.markdown("---")
                st.markdown("### 📊 AI Strategic Executive Report")
                st.write(response.text)
                st.balloons()

else:
    st.info("💡 Hub Ready. Upload your MLS files to begin analysis.")
