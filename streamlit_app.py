import streamlit as st
from datetime import date
from backend.etl import run_etl_batch
from backend.core.reports import MarketReports
from backend.ui.styles import apply_premium_style

# 1. Page Setup
st.set_page_config(page_title="Market Lens Enterprise", layout="wide", initial_sidebar_state="expanded")
apply_premium_style()

reports = MarketReports()

# 2. Sidebar Navigation (Google AI Studio Style)
with st.sidebar:
    st.markdown("<h1 style='font-size: 24px; color: #1E293B;'>Market Lens</h1>", unsafe_allow_html=True)
    
    if st.button("➕ New Report", use_container_width=True): st.session_state.view = 'New Report'
    if st.button("📑 Reports Hub", use_container_width=True): st.session_state.view = 'Reports'
    
    st.markdown("<div style='height:1px; background:#E2E8F0; margin:15px 0;'></div>", unsafe_allow_html=True)
    
    st.markdown("<p style='font-size:11px; font-weight:700; color:#94A3B8; letter-spacing:1px; margin-left:5px;'>ANALYTICS</p>", unsafe_allow_html=True)
    if st.button("🏠 Properties", use_container_width=True): st.session_state.view = 'Properties'
    if st.button("🌳 Land", use_container_width=True): st.session_state.view = 'Land'
    if st.button("🏢 Rental", use_container_width=True): st.session_state.view = 'Rental'
    
    st.markdown("<div style='height:1px; background:#E2E8F0; margin:15px 0;'></div>", unsafe_allow_html=True)
    
    # Report Selector
    st.subheader("Active Report")
    saved = reports.list_all_reports()
    if not saved.empty:
        opts = {f"{r['report_name']}": r['import_id'] for _, r in saved.iterrows()}
        selected_report = st.selectbox("Current Analysis:", options=list(opts.keys()), label_visibility="collapsed")
        st.session_state.active_report_id = opts[selected_report]
    else:
        st.caption("No data available.")

# 3. Workspace Routing
view = st.session_state.get('view', 'Properties')

if view == 'New Report':
    st.title("Data Ingestion & Classification")
    st.markdown("<div class='main-card'>", unsafe_allow_html=True)
    files = st.file_uploader("Upload MLS Files (Max 25)", accept_multiple_files=True, type=['csv', 'xlsx'])
    
    if files:
        report_name = st.text_input("Report Name", placeholder="e.g. Venice Beach Q1 2026")
        files_data = []
        st.markdown("### 📋 Classification Queue")
        for f in files:
            c1, c2 = st.columns([3, 1])
            c1.markdown(f"**📄 {f.name}**")
            f_type = c2.selectbox("Classify as:", ["Properties", "Land", "Rental"], key=f"type_{f.name}")
            files_data.append({'file': f, 'type': f_type})
        
        if st.button("🚀 Run Batch ETL", type="primary", use_container_width=True):
            res = run_etl_batch(files_data=files_data, report_name=report_name, snapshot_date=date.today(), contract_path="backend/contract/mls_column_contract.yaml")
            if res.ok: 
                st.success("Report Generated!")
                st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)

elif view in ['Properties', 'Land', 'Rental']:
    if st.session_state.get('active_report_id'):
        df = reports.load_report_data(st.session_state.active_report_id, view)
        st.title(f"{view} Analytics")
        
        if not df.empty:
            # TOP TABS: Overview, Compare, and ZIPs
            zips = sorted([str(z) for z in df['zip'].unique() if z])
            tab_list = ["📊 Overview", "⚖️ Compare"] + [f"📍 {z}" for z in zips]
            tabs = st.tabs(tab_list)
            
            with tabs[0]: # Overview
                st.markdown("<div class='main-card'>", unsafe_allow_html=True)
                st.dataframe(reports.get_inventory_overview(df), use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)
                
            with tabs[1]: # Compare
                st.markdown("<div class='main-card'>", unsafe_allow_html=True)
                st.subheader("ZIP Code Comparison Matrix")
                st.table(reports.get_comparison_matrix(df))
                st.markdown("</div>", unsafe_allow_html=True)

            for i, zip_code in enumerate(zips):
                with tabs[i+2]:
                    st.markdown("<div class='main-card'>", unsafe_allow_html=True)
                    zip_df = df[df['zip'].astype(str) == zip_code]
                    st.metric("Total Listings", len(zip_df))
                    st.dataframe(zip_df, use_container_width=True)
                    st.markdown("</div>", unsafe_allow_html=True)
        else:
            st.warning(f"No {view} found in this report.")
    else:
        st.info("Please select a report in the sidebar.")
