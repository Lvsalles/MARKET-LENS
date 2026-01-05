import streamlit as st
from datetime import date
from backend.etl import run_etl_batch
from backend.core.reports import MarketReports
from backend.ui.styles import apply_premium_style

# Configuration
st.set_page_config(page_title="Market Lens Enterprise", layout="wide")
apply_premium_style()

reports = MarketReports()

# Session State for File Queue
if 'file_queue' not in st.session_state: st.session_state.file_queue = {}

# --- SIDEBAR ---
with st.sidebar:
    st.markdown("<h2 style='color:#4F46E5;'>Market Lens</h2>", unsafe_allow_html=True)
    if st.button("➕ New Report", use_container_width=True, type="primary"):
        st.session_state.current_view = 'New Report'
    
    st.markdown("---")
    st.subheader("Saved Reports")
    try:
        saved = reports.list_all_reports()
        if not saved.empty:
            report_options = {f"{r['report_name']}": r['import_id'] for _, r in saved.iterrows()}
            sel = st.selectbox("Active Report:", options=list(report_options.keys()))
            st.session_state.active_report_id = report_options[sel]
    except: st.error("DB Error")

# --- MAIN WORKSPACE ---
view = st.session_state.get('current_view', 'Properties')

if view == 'New Report':
    st.title("Market Intelligence Ingestion")
    st.markdown("Upload up to 25 files. Each file must be classified before processing.")

    # 1. Multi-file Uploader
    uploaded_files = st.file_uploader("Drop MLS files (CSV/XLSX)", type=["csv", "xlsx"], accept_multiple_files=True)
    
    if uploaded_files:
        if len(uploaded_files) > 25:
            st.error("Maximum 25 files allowed.")
        else:
            report_name = st.text_input("Final Report Name", placeholder="e.g. Florida Annual Analysis 2025")
            
            st.markdown("### 📋 Classification Queue")
            
            # 2. Classification Grid
            files_to_process = []
            cols = st.columns(2) # Two cards per row
            
            for idx, f in enumerate(uploaded_files):
                with cols[idx % 2]:
                    st.markdown(f"""
                        <div style='background: white; padding: 15px; border-radius: 12px; border: 1px solid #E2E8F0; margin-bottom: 10px;'>
                            <b style='color: #1E293B;'>📄 {f.name}</b><br>
                            <small style='color: #64748B;'>Size: {f.size/1024:.1f} KB</small>
                        </div>
                    """, unsafe_allow_html=True)
                    
                    # Classification selector for THIS specific file
                    f_type = st.selectbox(
                        f"Content Type for {f.name}", 
                        ["Properties", "Land", "Rental", "Marketing"], 
                        key=f"type_{f.name}",
                        label_visibility="collapsed"
                    )
                    files_to_process.append({'file': f, 'type': f_type})
            
            st.markdown("---")
            
            # 3. Execution Action
            if st.button("🚀 Run Batch ETL & Generate Report", type="primary", use_container_width=True):
                if not report_name:
                    st.warning("Please name your report.")
                else:
                    with st.spinner(f"Ingesting {len(files_to_process)} files..."):
                        res = run_etl_batch(
                            files_data=files_to_process, 
                            report_name=report_name, 
                            snapshot_date=date.today(),
                            contract_path="backend/contract/mls_column_contract.yaml"
                        )
                        if res.ok:
                            st.success("Batch Processing Complete!")
                            st.session_state.active_report_id = res.import_id
                            st.session_state.current_view = 'Properties'
                            st.rerun()
                        else:
                            st.error(res.error)

elif view == 'Properties':
    if 'active_report_id' in st.session_state:
        df = reports.load_report_data(st.session_state.active_report_id)
        st.title("Property Analytics")
        if not df.empty:
            st.dataframe(df, use_container_width=True)
        else:
            st.info("No data in this report.")
    else:
        st.info("Select or create a report in the sidebar.")
