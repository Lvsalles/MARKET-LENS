import streamlit as st
import pandas as pd
import google.generativeai as genai
from pypdf import PdfReader
from docx import Document
import folium
from streamlit_folium import folium_static
import numpy as np

# 1. Configuração de Página
st.set_page_config(page_title="AI Investor Hub", layout="wide")

# 2. Configuração da AI (Auto-healing para 404)
if "GOOGLE_API_KEY" not in st.secrets:
    st.error("🔑 API Key Missing in Secrets.")
    st.stop()

genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])

@st.cache_resource
def get_ai_model():
    try:
        available = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
        target = 'models/gemini-1.5-flash' if 'models/gemini-1.5-flash' in available else available[0]
        return genai.GenerativeModel(target)
    except: return None

model = get_ai_model()

# ---------------------------------------------------------
# INVESTOR STANDARDIZATION LIBRARY
# ---------------------------------------------------------
# Mapeamento exato baseado na estrutura dos seus arquivos MLS
COLUMN_MAP = {
    'Price': ['Current Price', 'Current Price_num', 'List Price', 'Sold Price'],
    'Status': ['Status', 'Listing Status', 'LSC List Side', 'Status_clean'],
    'Address': ['Address', 'Full Address', 'Street Address'],
    'Zip': ['Zip', 'Zip Code'],
    'Subdivision': ['Legal Subdivision Name', 'Subdivision/Condo Name'],
    'Beds': ['Beds', 'Beds_num', 'Bedrooms'],
    'Baths': ['Full Baths', 'Full Baths_num', 'Bathrooms'],
    'SqFt': ['Heated Area', 'Heated Area_num', 'SqFt'],
    'Year': ['Year Built', 'Year Built_num'],
    'DOM': ['CDOM_num', 'ADOM_num', 'Days to Contract_num', 'CDOM', 'ADOM'],
    'Pool': ['Pool', 'Pool Features', 'Private Pool'],
    'Financing': ['Sold Terms', 'Terms', 'Financing'],
    'Zoning': ['Zoning', 'Zoning Code'],
    'Acreage': ['Total Acreage_num', 'Total Acreage'],
    'Agent': ['List Agent', 'Listing Agent']
}

STATUS_MAP = {
    'ACT': 'Active', 'SLD': 'Sold', 'PND': 'Pending', 'Closed': 'Sold', 'Active': 'Active'
}

def investor_normalize(df, filename):
    # 1. Rename columns
    for std_col, syns in COLUMN_MAP.items():
        found = next((c for c in syns if c in df.columns), None)
        if found:
            df = df.rename(columns={found: std_col})
    
    # 2. Cleanup duplicates (Keep last)
    df = df.loc[:, ~df.columns.duplicated(keep='last')]
    
    # 3. Translate Status (Fixed the NameError here)
    if 'Status' in df.columns:
        df['Status'] = df['Status'].map(STATUS_MAP).fillna(df['Status'])
    
    # 4. Numeric Cleaning
    for col in ['Price', 'SqFt', 'DOM', 'Beds', 'Baths', 'Year', 'Acreage']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(r'[$,]', '', regex=True), errors='coerce')
    
    # 5. Smart Metrics
    if 'Price' in df.columns and 'SqFt' in df.columns:
        df['PPSqFt'] = df['Price'] / df['SqFt']

    # 6. Category Tagging
    cat = "Residential"
    if "land" in filename.lower() or "acreage" in str(df.columns).lower(): cat = "Land"
    elif "rent" in filename.lower() or "lease" in str(df.columns).lower(): cat = "Rental"
    
    return df, cat

# ---------------------------------------------------------
# APP UI
# ---------------------------------------------------------
st.sidebar.title("💎 Investor Terminal")
analysis_type = st.sidebar.selectbox(
    "Select Report Level",
    ["Executive Market Summary", "Micro-Market (Zip/Street)", "Yield & Arbitrage Analysis", "Agent Alpha Ranking"]
)

st.title("🏙️ Global Real Estate Intelligence Hub")
st.write(f"Level: **{analysis_type}** | Analyst: **AI Specialist**")
st.markdown("---")

files = st.file_uploader("Upload MLS Exports (CSV, XLSX) or Property Docs (PDF, DOCX)", accept_multiple_files=True)

if files:
    full_data = []
    text_context = ""
    
    for f in files:
        with st.expander(f"📁 Parsing: {f.name}"):
            ext = f.name.split('.')[-1].lower()
            try:
                if ext in ['csv', 'xlsx']:
                    df_raw = pd.read_csv(f) if ext == 'csv' else pd.read_excel(f)
                    df, category = investor_normalize(df_raw, f.name)
                    df['Source_Cat'] = category
                    full_data.append(df)
                    st.success(f"File categorized as {category}")
                    st.write(df['Status'].value_counts() if 'Status' in df.columns else "No Status found")
                elif ext == 'pdf':
                    reader = PdfReader(f)
                    text_context += " ".join([p.extract_text() for p in reader.pages[:10]])
            except Exception as e:
                st.error(f"Error: {e}")

    if full_data:
        master_df = pd.concat(full_data, ignore_index=True)

        # 1. VISUAL METRICS
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Database Rows", len(master_df))
        m2.metric("Avg Price", f"${master_df['Price'].mean():,.0f}")
        m3.metric("Avg $/SqFt", f"${master_df['PPSqFt'].mean():,.2f}")
        m4.metric("Active Supply", len(master_df[master_df['Status']=='Active']))

        # 2. GEOSPATIAL MAP (Google Maps Integration Style)
        st.subheader("📍 Market Density & Heatmap")
        # We simulate a location for North Port if lat/long are missing
        m = folium.Map(location=[27.05, -82.25], zoom_start=11)
        for _, row in master_df.dropna(subset=['Address']).head(60).iterrows():
            color = 'blue' if row['Source_Cat'] == 'Residential' else 'green'
            folium.Marker(
                [27.05 + np.random.uniform(-0.06, 0.06), -82.25 + np.random.uniform(-0.06, 0.06)],
                popup=f"Price: ${row.get('Price', 0):,.0f}<br>Zip: {row.get('Zip','N/A')}",
                icon=folium.Icon(color=color, icon='info-sign')
            ).add_to(m)
        folium_static(m)

        # 3. DEEP DIVE BUTTON
        if st.button("🚀 Run Specialist Comparative Analysis"):
            with st.spinner('AI analyzing zoning, financing velocity, and zip hotspots...'):
                # Build context for AI with summarized statistics
                stats = {
                    "zips": master_df.groupby('Zip')['Price'].agg(['mean', 'count']).head(10).to_dict(),
                    "financing": master_df['Financing'].value_counts().to_dict() if 'Financing' in master_df.columns else {},
                    "subdivisions": master_df['Subdivision'].value_counts().head(10).to_dict()
                }

                prompt = f"""
                As a Real Estate Specialist and Investor, analyze the following real market data:
                
                LEVEL OF ANALYSIS: {analysis_type}
                DATA SUMMARY: {stats}
                ADDITIONAL CONTEXT: {text_context[:2000]}

                INSTRUCTIONS:
                1. MICRO-LEVEL: Compare Zipcodes. Which ones are undervalued based on Price per SqFt?
                2. AMENITY PREMIUM: How do pools or larger bed/bath counts affect the "Sold" speed?
                3. FINANCING VELOCITY: Compare Cash vs Conventional. Who closes faster and with more discount?
                4. ZONING & ADU: Mention ADU potential for subdivisions found (e.g. North Port estates).
                5. STREET ALPHA: Identify "Hot Streets" if address data shows patterns.
                6. 5 STRATEGIC PLAYS: Provide 5 professional investment strategies based on these files.

                Language: Professional English. Format: Markdown with bold headers.
                """
                
                response = model.generate_content(prompt)
                st.markdown("---")
                st.markdown("### 📊 AI Strategic Specialist Report")
                st.write(response.text)
                st.balloons()
