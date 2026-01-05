import streamlit as st
from datetime import date
from backend.etl import run_etl_batch
from backend.core.reports import MarketReports
from backend.ui.styles import apply_premium_style

# Initialize App
st.set_page_config(page_title="Market Lens Enterprise", layout="wide")
apply_premium_style()

reports = MarketReports()

# Navigation State
if 'current_view' not in st.session_state: st.session_state.current_view = 'Reports'

# --- SIDEBAR NAVIGATION ---
with st.sidebar:
    st.markdown("<h2 style='color:#4F46E5;'>Market Lens</h2>", unsafe_allow_html=True)
    if st.button("➕ Create New Report", use_container_width=True, type="primary"):
        st.session_state.current_view = 'New Report'
    
    st.markdown("---")
    st.subheader("Select Saved Report")
    try:
        saved = reports.list_all_reports()
        if not saved.empty:
            report_options = {f"{r['report_name']} ({r['snapshot_date']})": r['import_id'] for _, r in saved.iterrows()}
            selected_label = st.selectbox("Switch View:", options=list(report_options.keys()))
            st.session_state.active_report_id = report_options[selected_label]
            if st.button("👁️ View Report", use_container_width=True):
                st.session_state.current_view = 'Properties'
        else:
            st.caption("No reports available.")
    except:
        st.error("Database connection failed.")

# --- MAIN CONTENT AREA ---
view = st.session_state.current_view

if view == 'New Report':
    st.title("Market Intelligence Ingestion")
    st.caption("Upload up to 25 files. Each file must be classified for the isolated report.")

    uploaded_files = st.file_uploader("Drop MLS files here (CSV/XLSX)", type=["csv", "xlsx"], accept_multiple_files=True)
    
    if uploaded_files:
        if len(uploaded_files) > 25:
            st.error("Limit exceeded: Max 25 files.")
        else:
            report_name = st.text_input("Enter Report Name", placeholder="e.g. North Port Yearly Analysis 2025")
            
            st.markdown("### 📋 Classification Queue")
            files_to_process = []
            
            # Display files in a clean grid
            grid_cols = st.columns(2)
            for idx, f in enumerate(uploaded_files):
                with grid_cols[idx % 2]:
                    st.markdown(f"""
                        <div style='background: white; padding: 15px; border-radius: 12px; border: 1px solid #E2E8F0; margin-bottom: 5px;'>
                            <b style='color: #1E293B;'>📄 {f.name}</b><br>
                            <small>Size: {f.size/1024:.1f} KB</small>
                        </div>
                    """, unsafe_allow_html=True)
                    
                    f_type = st.selectbox(
                        f"Select type for {f.name}", 
                        ["Properties", "Land", "Rental", "Market Analysis"], 
                        key=f"type_{f.name}", label_visibility="collapsed"
                    )
                    files_to_process.append({'file': f, 'type': f_type})
            
            st.markdown("---")
            if st.button("🚀 Run Batch ETL & Generate Isolated Report", type="primary", use_container_width=True):
                if not report_name:
                    st.warning("Please name your report before processing.")
                else:
                    with st.spinner(f"Ingesting {len(files_to_process)} files..."):
                        res = run_etl_batch(
                            files_data=files_to_process, 
                            report_name=report_name, 
                            snapshot_date=date.today(),
                            contract_path="backend/contract/mls_column_contract.yaml"
                        )
                        if res.ok:
                            st.success("Batch ETL Completed!")
                            st.session_state.active_report_id = res.import_id
                            st.session_state.current_view = 'Properties'
                            st.rerun()
                        else:
                            st.error(res.error)

elif view == 'Properties':
    if 'active_report_id' in st.session_state:
        df = reports.load_report_data(st.session_state.active_report_id)
        st.title("Market Analysis Workspace")
        st.caption(f"Active Report ID: {st.session_state.active_report_id}")
        
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
                st.dataframe(year_df.style.format({"AVERAGE VALUE": "${:,.2f}"}), use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.info("Please select or create a report from the sidebar.")

else:
    st.title("Welcome to Market Lens")
    st.write("Select a saved report from the sidebar or create a new one.")
