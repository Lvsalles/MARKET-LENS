import streamlit as st
from datetime import date
from backend.etl import run_etl_batch
from backend.core.reports import MarketReports
from backend.ui.styles import apply_premium_style

# 1. Setup
st.set_page_config(page_title="Market Lens", layout="wide", initial_sidebar_state="expanded")
apply_premium_style()
reports = MarketReports()

# State
if 'view' not in st.session_state: st.session_state.view = 'Properties'
if 'active_report_id' not in st.session_state: st.session_state.active_report_id = None

# 2. Premium Sidebar (Left Navigation)
with st.sidebar:
    st.markdown("<h2 style='color:#1E293B; margin-left:10px;'>Market Lens</h2>", unsafe_allow_html=True)
    
    if st.button("✨ New Report", use_container_width=True): st.session_state.view = 'New Report'
    if st.button("📑 All Reports", use_container_width=True): st.session_state.view = 'Reports'
    
    st.markdown("<div class='thin-sep'></div>", unsafe_allow_html=True)
    
    # Categories
    st.markdown("<p style='font-size:11px; font-weight:700; color:#94A3B8; margin-left:15px;'>ANALYTICS</p>", unsafe_allow_html=True)
    if st.button("🏠 Properties", use_container_width=True): st.session_state.view = 'Properties'
    if st.button("🌳 Land", use_container_width=True): st.session_state.view = 'Land'
    if st.button("🏢 Rental", use_container_width=True): st.session_state.view = 'Rental'
    
    st.markdown("<div class='thin-sep'></div>", unsafe_allow_html=True)
    
    # Report Selector
    st.subheader("Active Workspace")
    saved = reports.list_all_reports()
    if not saved.empty:
        opts = {f"{r['report_name']}": r['import_id'] for _, r in saved.iterrows()}
        selected = st.selectbox("Select Report:", options=list(opts.keys()), label_visibility="collapsed")
        st.session_state.active_report_id = opts[selected]
    else:
        st.caption("No reports available.")

# 3. Main Workspace Routing
view = st.session_state.view

if view == 'New Report':
    st.title("Create Isolated Report")
    st.markdown("<div class='main-card'>", unsafe_allow_html=True)
    files = st.file_uploader("Upload up to 25 files", accept_multiple_files=True, type=['csv', 'xlsx'])
    if files:
        report_name = st.text_input("Report Name", placeholder="e.g. North Port 2025")
        files_data = []
        for f in files:
            c1, c2 = st.columns([3, 1])
            c1.markdown(f"**📄 {f.name}**")
            f_type = c2.selectbox("Type", ["Properties", "Land", "Rental"], key=f"type_{f.name}")
            # Map UI to DB names
            mapping = {"Properties": "residential_sale", "Land": "land", "Rental": "rental"}
            files_data.append({'file': f, 'type': mapping[f_type]})
        
        if st.button("🚀 Run Batch ETL", type="primary"):
            res = run_etl_batch(files_data=files_data, report_name=report_name, snapshot_date=date.today(), contract_path="backend/contract/mls_column_contract.yaml")
            if res.ok: st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)

elif view in ['Properties', 'Land', 'Rental']:
    if st.session_state.active_report_id:
        df = reports.load_report_data(st.session_state.active_report_id, view)
        st.title(f"{view} Analytics")
        
        if not df.empty:
            # TOP TABS
            unique_zips = sorted([str(z) for z in df['zip'].unique() if z])
            tab_labels = ["📊 Overview", "⚖️ Compare"] + [f"📍 {z}" for z in unique_zips]
            tabs = st.tabs(tab_labels)
            
            with tabs[0]:
                st.markdown("<div class='main-card'>", unsafe_allow_html=True)
                st.dataframe(reports.get_inventory_overview(df), use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)
                
            with tabs[1]:
                st.markdown("<div class='main-card'>", unsafe_allow_html=True)
                st.write("Comparison Matrix logic here...")
                st.markdown("</div>", unsafe_allow_html=True)

            for i, zip_code in enumerate(unique_zips):
                with tabs[i+2]:
                    st.markdown("<div class='main-card'>", unsafe_allow_html=True)
                    st.subheader(f"Region: {zip_code}")
                    st.dataframe(df[df['zip'].astype(str) == zip_code], use_container_width=True)
                    st.markdown("</div>", unsafe_allow_html=True)
        else:
            st.info(f"No {view} data in this report.")
    else:
        st.warning("Select a report in the sidebar.")
