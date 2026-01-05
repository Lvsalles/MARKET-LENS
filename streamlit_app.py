import streamlit as st
import pandas as pd
from datetime import date
from backend.etl import run_etl
from backend.core.reports import MarketReports
from backend.ui.styles import apply_premium_style

# 1. Initialize Page Config
st.set_page_config(
    page_title="Market Lens | Real Estate Intelligence", 
    layout="wide", 
    initial_sidebar_state="expanded"
)

# Apply custom CSS from our style module
apply_premium_style()

# 2. State Management
if 'current_view' not in st.session_state: 
    st.session_state.current_view = 'Reports'
if 'etl_step' not in st.session_state: 
    st.session_state.etl_step = 'Idle'

# Helper to render status badges
def render_badge(status_text):
    status_class = "badge-locked"
    if "Ready" in status_text:
        status_class = "badge-ready"
    elif "Setup" in status_text:
        status_class = "badge-setup"
    
    clean_text = status_text.split()[-1]
    return f"<span class='badge {status_class}'>{clean_text}</span>"

# 3. Sidebar Navigation
with st.sidebar:
    st.markdown("<h2 style='color:#4F46E5; margin-bottom: 20px;'>Market Lens</h2>", unsafe_allow_html=True)
    
    if st.button("➕ New Report", use_container_width=True, type="primary"):
        st.session_state.current_view = 'New Report'

    st.markdown("<br>", unsafe_allow_html=True)
    
    # Navigation Structure
    nav_structure = {
        "Reports": "🟢 Ready",
        "Properties": "🟢 Ready",
        "Land": "🟡 Setup",
        "Rental": "🔒 Locked",
        "Settings": "🟢 Ready"
    }
    
    for item, status in nav_structure.items():
        cols = st.columns([0.7, 0.3])
        # Navigation Button
        if cols[0].button(item, key=f"nav_{item}", use_container_width=True):
            st.session_state.current_view = item
        # Status Badge
        cols[1].markdown(render_badge(status), unsafe_allow_html=True)

# 4. Global Top Bar
t_col1, t_col2 = st.columns([4, 1])
with t_col1:
    st.text_input("🔍 Search Properties or Reports...", placeholder="Press Ctrl+K to search", label_visibility="collapsed")
with t_col2:
    st.markdown("<div style='text-align:right; font-size: 14px;'>Enterprise Workspace: <b>Market Lens v1.0</b></div>", unsafe_allow_html=True)

st.markdown("<hr style='margin-top: 0; margin-bottom: 25px; opacity: 0.1;'>", unsafe_allow_html=True)

# 5. Main Content Routing
view = st.session_state.current_view

# ---------------------------------------------------------
# VIEW: NEW REPORT (Guided ETL Workflow)
# ---------------------------------------------------------
if view == 'New Report':
    st.title("Data Ingestion Engine")
    st.caption("Upload and classify new datasets to update market intelligence.")
    
    m_col1, m_col2 = st.columns([3, 1]) # Workspace | Action Panel
    
    with m_col1:
        st.markdown("<div class='main-card'>", unsafe_allow_html=True)
        uploaded_file = st.file_uploader("Drop MLS files here (CSV, XLSX)", type=["csv", "xlsx"], label_visibility="collapsed")
        
        if uploaded_file:
            st.markdown("### File Identification")
            c1, c2, c3 = st.columns(3)
            c1.info(f"**Name:** {uploaded_file.name}")
            c2.info(f"**Size:** {uploaded_file.size/1024:.1f} KB")
            asset_type = c3.selectbox("Asset Class", ["Residential Properties", "Land", "Commercial", "Rentals"])
            
            st.markdown("---")
            st.markdown("#### Schema Preview")
            # Show top 5 rows
            temp_df = pd.read_csv(uploaded_file) if uploaded_file.name.endswith('.csv') else pd.read_excel(uploaded_file)
            st.dataframe(temp_df.head(3), use_container_width=True)
        else:
            st.write("Waiting for file upload...")
        st.markdown("</div>", unsafe_allow_html=True)

    with m_col2:
        st.markdown("### Next Steps")
        # Action Workflow Panel
        steps = [
            ("Validate Schema", "Done" if uploaded_file else "Pending"),
            ("Normalize Columns", "Pending" if uploaded_file else "Locked"),
            ("Assign Asset Class", "Pending" if uploaded_file else "Locked"),
            ("Execute ETL", "Locked")
        ]
        
        for step, status in steps:
            icon = "✅" if status == "Done" else "🟡" if status == "Pending" else "🔒"
            st.button(f"{icon} {step}", key=f"step_{step}", disabled=(status == "Locked"), use_container_width=True)
        
        if uploaded_file:
            if st.button("🚀 Run Market Update", type="primary", use_container_width=True):
                with st.spinner("Processing Data..."):
                    res = run_etl(xlsx_file=uploaded_file, snapshot_date=date.today(), contract_path="backend/contract/mls_column_contract.yaml")
                    if res.ok:
                        st.success("Intelligence Updated!")
                        st.balloons()
                    else:
                        st.error(f"ETL Error: {res.error}")

# ---------------------------------------------------------
# VIEW: PROPERTIES (Analytics & ZIP Tabs)
# ---------------------------------------------------------
elif view == 'Properties':
    reports = MarketReports()
    try:
        df = reports.load_data()
        
        st.title("Property Market Analytics")
        
        if not df.empty:
            # ZIP Code Navigation
            zips = ["Overview"] + sorted([str(z) for z in df['zip'].unique().tolist()])
            zip_tabs = st.tabs(zips)
            
            for i, tab in enumerate(zip_tabs):
                with tab:
                    if zips[i] == "Overview":
                        st.markdown("<div class='main-card'>", unsafe_allow_html=True)
                        st.subheader("Global Inventory Summary")
                        st.dataframe(reports.get_inventory_overview(df), use_container_width=True)
                        st.markdown("</div>", unsafe_allow_html=True)
                    else:
                        st.subheader(f"Region Insights: {zips[i]}")
                        zip_df = df[df['zip'].astype(str) == zips[i]]
                        
                        kpi1, kpi2, kpi3 = st.columns(3)
                        kpi1.metric("Listings", len(zip_df))
                        kpi2.metric("Avg Price", f"${zip_df['list_price'].mean():,.0f}")
                        kpi3.metric("Avg SQFT", f"{zip_df['heated_area'].mean():,.0f}")
                        
                        st.dataframe(zip_df, use_container_width=True)
        else:
            st.warning("No data found. Please run a New Report to ingest MLS data.")
    except Exception as e:
        st.error(f"System Error: {e}")

# ---------------------------------------------------------
# DEFAULT VIEW (Reports)
# ---------------------------------------------------------
else:
    st.title("Market Intelligence Reports")
    st.info("Select a module from the sidebar to view detailed analytics.")
