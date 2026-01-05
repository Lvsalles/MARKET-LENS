import streamlit as st
import pandas as pd
from datetime import date
from backend.etl import run_etl
from backend.core.reports import MarketReports
from backend.ui.styles import apply_premium_style

# 1. Initialize Page Config
st.set_page_config(page_title="Market Lens | Real Estate Intelligence", layout="wide", initial_sidebar_state="expanded")
apply_premium_style()

# 2. State Management
if 'current_view' not in st.session_state: st.session_state.current_view = 'Reports'
if 'etl_step' not in st.session_state: st.session_state.etl_step = 'Idle'

# 3. Sidebar Navigation (Structured as requested)
with st.sidebar:
    st.markdown("<h2 style='color:#4F46E5;'>Market Lens</h2>", unsafe_allow_html=True)
    
    if st.button("➕ New Report", use_container_width=True, type="primary"):
        st.session_state.show_modal = True
        st.session_state.current_view = 'New Report'

    st.markdown("---")
    
    # Navigation Items with Status Badges
    nav_items = {
        "Reports": "🟢 Ready",
        "Properties": "🟢 Ready",
        "Land": "🟡 Setup",
        "Rental": "🔒 Locked",
        "Settings": "🟢 Ready"
    }
    
    for item, status in nav_items.items():
        cols = st.columns([0.7, 0.3])
        if cols[0].button(item, key=f"nav_{item}", use_container_width=True):
            st.session_state.current_view = item
        cols[1].markdown(f"<span class='badge {"badge-ready" if "Ready" in status else "badge-setup" if "Setup" in status else "badge-locked"}'>{status.split()[-1]}</span>", unsafe_allow_html=True)

# 4. Top Bar Simulation
t_col1, t_col2 = st.columns([4, 1])
with t_col1:
    st.text_input("🔍 Global Search (Cmd + K)", placeholder="Search properties, reports, or agents...", label_visibility="collapsed")
with t_col2:
    st.markdown("<div style='text-align:right;'>👤 <b>Principal Architect</b></div>", unsafe_allow_html=True)

st.markdown("---")

# 5. Main Workspace Routing
view = st.session_state.current_view

# ---------------------------------------------------------
# VIEW: NEW REPORT (ETL Workflow)
# ---------------------------------------------------------
if view == 'New Report':
    st.header("Create New Market Report")
    st.info("Follow the guided steps to ingest MLS data into the intelligence engine.")
    
    m_col1, m_col2 = st.columns([3, 1]) # Workspace | Action Panel
    
    with m_col1:
        st.markdown("<div class='main-card'>", unsafe_allow_html=True)
        uploaded_file = st.file_uploader("Drag and drop MLS files (CSV or XLSX)", type=["csv", "xlsx"])
        
        if uploaded_file:
            st.subheader("File Classification")
            c1, c2, c3 = st.columns(3)
            c1.text(f"📄 {uploaded_file.name}")
            c2.text(f"⚖️ {uploaded_file.size/1024:.1f} KB")
            asset_type = c3.selectbox("Dataset Category", ["Properties", "Land", "Rental", "Market Analysis"])
            
            st.markdown("---")
            st.subheader("Schema Validation")
            st.success("Columns detected: ML Number, Status, List Price, Heated Area...")
        st.markdown("</div>", unsafe_allow_html=True)

    with m_col2:
        st.markdown("### Next Actions")
        # Guided Action Panel
        steps = {
            "Validate Schema": "Done",
            "Normalize Columns": "Pending",
            "Assign Asset Class": "Pending",
            "Run ETL": "Locked"
        }
        
        for step, status in steps.items():
            st.button(f"{'✅' if status == 'Done' else '🟡' if status == 'Pending' else '🔒'} {step}", 
                      disabled=(status == "Locked"), use_container_width=True)
        
        if uploaded_file:
            if st.button("🚀 Execute ETL Process", type="primary", use_container_width=True):
                res = run_etl(xlsx_file=uploaded_file, snapshot_date=date.today(), contract_path="backend/contract/mls_column_contract.yaml")
                if res.ok: st.balloons()

# ---------------------------------------------------------
# VIEW: PROPERTIES (Data & ZIP Tabs)
# ---------------------------------------------------------
elif view == 'Properties':
    reports = MarketReports()
    df = reports.load_data()
    
    st.header("Properties Intelligence")
    
    # State-driven ZIP Tabs
    if not df.empty:
        zips = ["Overview"] + sorted(df['zip'].unique().tolist())
        zip_tabs = st.tabs(zips)
        
        for i, tab in enumerate(zip_tabs):
            with tab:
                if zips[i] == "Overview":
                    st.subheader("Global Market Summary")
                    st.dataframe(reports.get_inventory_overview(df), use_container_width=True)
                else:
                    st.subheader(f"Analysis for ZIP: {zips[i]}")
                    zip_df = df[df['zip'] == zips[i]]
                    st.metric("Active Listings", len(zip_df))
                    st.dataframe(zip_df, use_container_width=True)
    else:
        st.warning("No data available. Run a New Report to begin.")

# ---------------------------------------------------------
# VIEW: SETTINGS
# ---------------------------------------------------------
elif view == 'Settings':
    st.header("Organization Settings")
    with st.expander("User Management", expanded=True):
        st.write("Current User: admin@marketlens.com (SuperAdmin)")
        st.button("Invite Team Member")
    with st.expander("Data Preferences"):
        st.checkbox("Auto-detect Currency", value=True)
        st.checkbox("Enable AI Insights on Import", value=True)

# ---------------------------------------------------------
# ERROR HANDLING (Right Side Slide-out Simulation)
# ---------------------------------------------------------
# If an error was in session state, we would show a Red Action Panel here.
