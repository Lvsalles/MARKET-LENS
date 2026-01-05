import streamlit as st
from datetime import date
from backend.etl import run_batch_etl
from backend.core.reports import MarketReports
from backend.ui.styles import apply_premium_style

# 1. SETUP
st.set_page_config(page_title="Market Lens Enterprise", layout="wide", initial_sidebar_state="expanded")
apply_premium_style()
reports = MarketReports()

# 2. STATE CONTROLLER (The fix for "No Active Report")
if 'view' not in st.session_state: st.session_state.view = 'Properties'
if 'active_id' not in st.session_state: st.session_state.active_id = None

# --- SIDEBAR ---
with st.sidebar:
    st.markdown("<h1 style='font-size: 24px; color: #1E293B;'>Market Lens</h1>", unsafe_allow_html=True)
    
    if st.button("✨ New Report", use_container_width=True): 
        st.session_state.view = 'New Report'
    
    st.markdown("<div class='nav-sep'></div>", unsafe_allow_html=True)
    st.caption("ANALYTICS")
    if st.button("🏠 Properties", use_container_width=True): st.session_state.view = 'Properties'
    if st.button("🌳 Land", use_container_width=True): st.session_state.view = 'Land'
    if st.button("🏢 Rental", use_container_width=True): st.session_state.view = 'Rental'

    st.markdown("<div class='nav-sep'></div>", unsafe_allow_html=True)
    
    # REPORT SELECTOR
    st.subheader("Active Report")
    try:
        saved_df = reports.list_all_reports()
        if not saved_df.empty:
            # Create dictionary mapping names to IDs
            report_map = {f"{r['report_name']}": r['import_id'] for _, r in saved_df.iterrows()}
            
            # Find index of current active_id to keep it selected
            current_index = 0
            if st.session_state.active_id:
                for idx, (name, id) in enumerate(report_map.items()):
                    if id == st.session_state.active_id:
                        current_index = idx
            
            selected_name = st.selectbox("Switch View:", options=list(report_map.keys()), index=current_index)
            st.session_state.active_id = report_map[selected_name]
        else:
            st.info("No reports found.")
    except:
        st.error("Database connection error.")

# --- MAIN WORKSPACE ---
view = st.session_state.view

if view == 'New Report':
    st.title("Market Intelligence Ingestion")
    st.markdown("<div class='main-card'>", unsafe_allow_html=True)
    files = st.file_uploader("Upload MLS Files (Max 25)", accept_multiple_files=True, type=['csv', 'xlsx'])
    
    if files:
        report_name = st.text_input("Analysis Name", placeholder="e.g. North Port 2025")
        st.markdown("### 📋 Classification Queue")
        files_data = []
        for f in files:
            c1, c2 = st.columns([3, 1])
            c1.markdown(f"**📄 {f.name}**")
            f_type = c2.selectbox("Type", ["Properties", "Land", "Rental"], key=f"type_{f.name}")
            files_data.append({'file': f, 'type': f_type})
        
        if st.button("🚀 Run Batch ETL", type="primary", use_container_width=True):
            if report_name:
                with st.spinner("Creating Silo..."):
                    res = run_batch_etl(files_data, report_name, date.today())
                    if res['ok']:
                        # FORCE STATE UPDATE
                        st.session_state.active_id = res['import_id']
                        st.session_state.view = 'Properties'
                        st.rerun()
                    else:
                        st.error(res['error'])
            else:
                st.warning("Please name your report.")
    st.markdown("</div>", unsafe_allow_html=True)

elif view in ['Properties', 'Land', 'Rental']:
    st.title(f"{view} Analytics")
    if st.session_state.active_id:
        # LOAD DATA FOR THIS SPECIFIC SILO AND CATEGORY
        df = reports.load_report_data(st.session_state.active_id, view)
        
        if not df.empty:
            zips = sorted([str(z) for z in df['zip'].unique() if z])
            tab_labels = ["📊 Overview", "⚖️ Compare"] + [f"📍 {z}" for z in zips]
            tabs = st.tabs(tab_labels)
            
            with tabs[0]: # Overview
                st.markdown("<div class='main-card'>", unsafe_allow_html=True)
                st.dataframe(reports.get_inventory_overview(df), use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)
                
            # Other tabs logic...
            for i, zip_code in enumerate(zips):
                with tabs[i+2]:
                    st.markdown("<div class='main-card'>", unsafe_allow_html=True)
                    st.dataframe(df[df['zip'].astype(str) == zip_code], use_container_width=True)
                    st.markdown("</div>", unsafe_allow_html=True)
        else:
            st.info(f"No {view} data found in the selected report silo.")
    else:
        st.warning("Please select a report in the sidebar.")
