import streamlit as st
from datetime import date
from backend.etl import run_etl_batch
from backend.core.reports import MarketReports
from backend.ui.styles import apply_premium_style

# Must be first
st.set_page_config(page_title="Market Lens Enterprise", layout="wide", initial_sidebar_state="expanded")
apply_premium_style()

reports = MarketReports()

# Initialize Navigation State
if 'view' not in st.session_state: st.session_state.view = 'Properties'
if 'active_report_id' not in st.session_state: st.session_state.active_report_id = None

# --- SIDEBAR NAVIGATION ---
with st.sidebar:
    st.markdown("<h1 style='font-size: 24px; color: #1E293B; margin-left:10px;'>Market Lens</h1>", unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    if st.button("➕ New Report", use_container_width=True): 
        st.session_state.view = 'New Report'
    
    st.markdown("<div class='nav-divider'></div>", unsafe_allow_html=True)
    st.markdown("<p style='font-size: 11px; font-weight: 700; color: #94A3B8; margin-left: 15px;'>ANALYTICS</p>", unsafe_allow_html=True)
    
    if st.button("🏠 Properties", use_container_width=True): st.session_state.view = 'Properties'
    if st.button("🌳 Land", use_container_width=True): st.session_state.view = 'Land'
    if st.button("🏢 Rental", use_container_width=True): st.session_state.view = 'Rental'

    st.markdown("<div class='nav-divider'></div>", unsafe_allow_html=True)
    
    # Report Selector
    st.subheader("Active Report")
    try:
        saved = reports.list_all_reports()
        if not saved.empty:
            opts = {f"{r['report_name']}": r['import_id'] for _, r in saved.iterrows()}
            sel = st.selectbox("Select Dataset:", options=list(opts.keys()), label_visibility="collapsed")
            st.session_state.active_report_id = opts[sel]
        else:
            st.caption("No reports available.")
    except:
        st.error("Database Connection Error")

# --- WORKSPACE ---
view = st.session_state.view

if view == 'New Report':
    st.title("Data Ingestion & Classification")
    st.markdown("<div class='main-card'>", unsafe_allow_html=True)
    files = st.file_uploader("Upload MLS Files (Max 25)", accept_multiple_files=True, type=['csv', 'xlsx'])
    
    if files:
        report_name = st.text_input("Enter a name for this Analysis", placeholder="e.g. North Port 2025")
        st.markdown("### 📋 Classification Queue")
        files_data = []
        for f in files:
            c1, c2 = st.columns([3, 1])
            c1.markdown(f"**📄 {f.name}**")
            f_type = c2.selectbox("Classify as:", ["Properties", "Land", "Rental"], key=f"type_{f.name}")
            files_data.append({'file': f, 'type': f_type})
        
        if st.button("🚀 Run Batch ETL & Generate Report", type="primary", use_container_width=True):
            if report_name:
                with st.spinner("Processing files and creating silo..."):
                    res = run_etl_batch(files_data=files_data, report_name=report_name, snapshot_date=date.today(), contract_path="backend/contract/mls_column_contract.yaml")
                    if res['ok']:
                        st.session_state.active_report_id = res['import_id']
                        st.session_state.view = 'Properties' # Redirect to Properties
                        st.rerun()
            else:
                st.warning("Please name your report.")
    st.markdown("</div>", unsafe_allow_html=True)

elif view in ['Properties', 'Land', 'Rental']:
    if st.session_state.active_report_id:
        # Load data isolated by ID and Category
        df = reports.load_report_data(st.session_state.active_report_id, view)
        st.title(f"{view} Analytics")
        
        if not df.empty:
            # DYNAMIC TOP TABS
            unique_zips = sorted([str(z) for z in df['zip'].unique() if z])
            tab_labels = ["📊 Overview", "⚖️ Compare"] + [f"📍 {z}" for z in unique_zips]
            tabs = st.tabs(tab_labels)
            
            with tabs[0]: # Overview
                st.markdown("<div class='main-card'>", unsafe_allow_html=True)
                st.subheader("Market Summary")
                st.dataframe(reports.get_inventory_overview(df), use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)
                
            with tabs[1]: # Compare
                st.markdown("<div class='main-card'>", unsafe_allow_html=True)
                st.subheader("ZIP Code Comparison")
                st.dataframe(reports.get_comparison_matrix(df), use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)

            # Individual ZIP Tabs
            for i, zip_code in enumerate(unique_zips):
                with tabs[i+2]:
                    st.markdown("<div class='main-card'>", unsafe_allow_html=True)
                    st.subheader(f"Region Insights: {zip_code}")
                    zip_df = df[df['zip'].astype(str) == zip_code]
                    st.metric("Active Listings", len(zip_df))
                    st.dataframe(zip_df, use_container_width=True)
                    st.markdown("</div>", unsafe_allow_html=True)
        else:
            st.info(f"No {view} data found in the selected report.")
    else:
        st.warning("Please select an active report in the sidebar.")
