import streamlit as st
from datetime import date
from backend.etl import run_etl
from backend.core.reports import MarketReports
from backend.ui.styles import apply_premium_style

st.set_page_config(page_title="Market Lens AI", layout="wide")
apply_premium_style()

reports = MarketReports()

# --- SIDEBAR: REPORT MANAGEMENT ---
with st.sidebar:
    st.markdown("<h2 style='color:#4F46E5;'>Market Lens</h2>", unsafe_allow_html=True)
    
    # 1. NEW REPORT TRIGGER
    if st.button("➕ New Report", use_container_width=True, type="primary"):
        st.session_state.current_view = 'New Report'

    st.markdown("---")
    
    # 2. ISOLATED REPORT SELECTOR
    st.subheader("Select Active Report")
    saved_reports = reports.list_all_reports()
    
    if not saved_reports.empty:
        report_options = {row['report_name']: row['import_id'] for _, row in saved_reports.iterrows()}
        selected_name = st.selectbox("Switch View:", options=list(report_options.keys()))
        st.session_state.active_report_id = report_options[selected_name]
        st.success(f"Viewing: {selected_name}")
    else:
        st.warning("No reports found.")

    st.markdown("---")
    if st.button("📊 Properties Workspace", use_container_width=True):
        st.session_state.current_view = 'Properties'

# --- MAIN WORKSPACE ---
view = st.session_state.get('current_view', 'Properties')

if view == 'New Report':
    st.title("Create Isolated Report")
    name = st.text_input("Report Name (e.g. Analysis North Port 2025)")
    file = st.file_uploader("Upload Data", type=["csv", "xlsx"])
    dt = st.date_input("Data Date", date.today())
    
    if st.button("🚀 Process and Save Report"):
        if name and file:
            res = run_etl(xlsx_file=file, report_name=name, snapshot_date=dt, contract_path="backend/contract/mls_column_contract.yaml")
            if res.ok:
                st.success(f"Report '{name}' saved separately!")
                st.session_state.current_view = 'Properties'
                st.rerun()

elif view == 'Properties':
    if 'active_report_id' in st.session_state:
        df = reports.load_report_data(st.session_state.active_report_id)
        
        st.title("Isolated Property Analytics")
        st.caption(f"Currently analyzing data for Report ID: {st.session_state.active_report_id}")
        
        if not df.empty:
            t1, t2 = st.tabs(["Overview", "ZIP Analysis"])
            with t1:
                st.dataframe(reports.get_inventory_overview(df), use_container_width=True)
            with t2:
                # Dynamic ZIP Tabs
                zips = sorted(df['zip'].unique().tolist())
                zip_tabs = st.tabs([str(z) for z in zips])
                for i, tab in enumerate(zip_tabs):
                    with tab:
                        st.dataframe(df[df['zip'] == zips[i]], use_container_width=True)
    else:
        st.info("Please create or select a report from the sidebar.")
