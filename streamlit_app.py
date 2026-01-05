import streamlit as st
from datetime import date
from backend.etl import run_etl
from backend.core.reports import MarketReports
from backend.ui.styles import apply_premium_style

st.set_page_config(page_title="Market Lens AI", layout="wide")
apply_premium_style()

reports = MarketReports()

# --- SIDEBAR: NAVIGATION & REPORT SELECTION ---
with st.sidebar:
    st.markdown("<h2 style='color:#4F46E5;'>Market Lens</h2>", unsafe_allow_html=True)
    
    if st.button("➕ New Report", use_container_width=True, type="primary"):
        st.session_state.current_view = 'New Report'

    st.markdown("---")
    
    st.subheader("Select Active Report")
    try:
        saved_reports = reports.list_all_reports()
        if not saved_reports.empty:
            report_options = {f"{row['report_name']} ({row['snapshot_date']})": row['import_id'] for _, row in saved_reports.iterrows()}
            selected_label = st.selectbox("Active Dataset:", options=list(report_options.keys()))
            st.session_state.active_report_id = report_options[selected_label]
            st.session_state.current_view = 'Properties'
        else:
            st.warning("No reports found.")
    except:
        st.error("Database connection error.")

    st.markdown("---")
    if st.button("📊 Analysis Workspace", use_container_width=True):
        st.session_state.current_view = 'Properties'

# --- MAIN WORKSPACE ---
view = st.session_state.get('current_view', 'Properties')

if view == 'New Report':
    st.title("Create New Isolated Report")
    st.markdown("<div class='main-card'>", unsafe_allow_html=True)
    
    report_name = st.text_input("Report Name", placeholder="e.g., Sarasota Q1 2025")
    uploaded_file = st.file_uploader("Upload MLS File (CSV or XLSX)", type=["csv", "xlsx"])
    snapshot_date = st.date_input("Analysis Date", date.today())
    
    if st.button("🚀 Process and Isolate Data", type="primary"):
        if report_name and uploaded_file:
            with st.spinner("Executing isolated ETL..."):
                res = run_etl(xlsx_file=uploaded_file, report_name=report_name, snapshot_date=snapshot_date, contract_path="backend/contract/mls_column_contract.yaml")
                if res.ok:
                    st.success(f"Report '{report_name}' created successfully!")
                    st.session_state.active_report_id = res.import_id
                    st.session_state.current_view = 'Properties'
                    st.rerun()
                else:
                    st.error(res.error)
        else:
            st.warning("Please provide both a name and a file.")
    st.markdown("</div>", unsafe_allow_html=True)

elif view == 'Properties':
    if 'active_report_id' in st.session_state:
        df = reports.load_report_data(st.session_state.active_report_id)
        
        st.title("Market Intelligence Dashboard")
        st.caption(f"Data Source: Isolated ID {st.session_state.active_report_id}")
        
        if not df.empty:
            t1, t2, t3 = st.tabs(["Inventory Overview", "Size Analysis", "Year/Price Analysis"])
            
            with t1:
                st.markdown("<div class='main-card'>", unsafe_allow_html=True)
                st.dataframe(reports.get_inventory_overview(df), use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)
            
            with t2:
                st.markdown("<div class='main-card'>", unsafe_allow_html=True)
                size_df = reports.get_size_analysis(df)
                st.dataframe(size_df.style.format({"AVERAGE VALUE": "${:,.2f}", "$/SQFT": "${:,.2f}"}), use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)

            with t3:
                st.markdown("<div class='main-card'>", unsafe_allow_html=True)
                year_df = reports.get_year_analysis(df)
                st.dataframe(year_df.style.format({"AVERAGE VALUE": "${:,.2f}", "$/SQFT": "${:,.2f}"}), use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)
        else:
            st.info("The selected report has no data.")
    else:
        st.info("Use the sidebar to create or select a report.")
