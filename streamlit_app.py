import streamlit as st
from datetime import date
from backend.etl import run_etl_batch
from backend.core.reports import MarketReports
from backend.ui.styles import apply_premium_style

# 1. Page Config & Design
st.set_page_config(page_title="Market Lens Enterprise", layout="wide", initial_sidebar_state="expanded")
apply_premium_style()

# 2. State Initialization
if 'view' not in st.session_state: st.session_state.view = 'Reports'
if 'active_report' not in st.session_state: st.session_state.active_report = None

reports = MarketReports()

# --- SIDEBAR NAVIGATION ---
with st.sidebar:
    st.markdown("<h2 style='color:#4F46E5; font-size: 24px; padding-left: 10px;'>Market Lens</h2>", unsafe_allow_html=True)
    
    if st.button("✨ New Report"): st.session_state.view = 'New Report'
    if st.button("📈 All Reports"): st.session_state.view = 'Reports'
    
    st.markdown("<p style='font-size: 12px; color: #94A3B8; margin-top: 20px; padding-left: 10px;'>ANALYTICS</p>", unsafe_allow_html=True)
    if st.button("🏠 Properties"): st.session_state.view = 'Properties'
    if st.button("🌳 Land"): st.session_state.view = 'Land'
    if st.button("🏢 Rental"): st.session_state.view = 'Rental'
    
    st.markdown("---")
    st.subheader("Active Report")
    saved = reports.list_all_reports()
    if not saved.empty:
        opts = {f"{r['report_name']} ({r['snapshot_date']})": r['import_id'] for _, r in saved.iterrows()}
        selected = st.selectbox("Switch Workspace:", options=list(opts.keys()), label_visibility="collapsed")
        st.session_state.active_report = opts[selected]
    else:
        st.caption("No reports created yet.")

# --- MAIN CONTENT ROUTING ---

# VIEW: NEW REPORT
if st.session_state.view == 'New Report':
    st.title("Market Intelligence Ingestion")
    st.caption("Guide: Upload up to 25 MLS files and classify them to generate an isolated intelligence report.")
    
    col_main, col_action = st.columns([3, 1])
    
    with col_main:
        st.markdown("<div class='main-card'>", unsafe_allow_html=True)
        files = st.file_uploader("Upload MLS Files", accept_multiple_files=True, type=['csv', 'xlsx'], label_visibility="collapsed")
        
        if files:
            st.markdown("### 📋 File Classification Queue")
            report_name = st.text_input("Report Name", placeholder="e.g. Sarasota Annual 2025")
            
            files_data = []
            for f in files:
                c1, c2 = st.columns([2, 1])
                c1.markdown(f"**📄 {f.name}**")
                f_type = c2.selectbox("Type", ["Properties", "Land", "Rental"], key=f"type_{f.name}")
                files_data.append({'file': f, 'type': f_type})
        st.markdown("</div>", unsafe_allow_html=True)

    with col_action:
        st.markdown("### Next Actions")
        # Visual Action Panel
        has_files = len(files) > 0 if files else False
        st.markdown(f"1. Validate Schema {'🟢' if has_files else '⚪'}")
        st.markdown(f"2. Normalize Data {'🟡' if has_files else '⚪'}")
        st.markdown(f"3. Run ETL {'🔒' if not has_files else '🟡'}")
        
        if files and report_name:
            if st.button("🚀 Execute Processing", type="primary", use_container_width=True):
                with st.spinner("Analyzing and Ingesting..."):
                    res = run_etl_batch(files_data=files_data, report_name=report_name, snapshot_date=date.today(), contract_path="backend/contract/mls_column_contract.yaml")
                    if res.ok:
                        st.session_state.active_report = res.import_id
                        st.session_state.view = 'Properties'
                        st.rerun()

# VIEW: PROPERTIES
elif st.session_state.view == 'Properties':
    if st.session_state.active_report:
        df = reports.load_report_data(st.session_state.active_report)
        st.title("Properties Workspace")
        
        if not df.empty:
            zips = ["Overview"] + sorted([str(z) for z in df['zip'].unique()])
            tabs = st.tabs(zips)
            
            for i, tab in enumerate(tabs):
                with tab:
                    if zips[i] == "Overview":
                        st.markdown("<div class='main-card'>", unsafe_allow_html=True)
                        st.subheader("Market Summary")
                        st.dataframe(reports.get_inventory_overview(df), use_container_width=True)
                        st.markdown("</div>", unsafe_allow_html=True)
                    else:
                        st.markdown("<div class='main-card'>", unsafe_allow_html=True)
                        st.subheader(f"ZIP Code {zips[i]} Analysis")
                        st.dataframe(df[df['zip'].astype(str) == zips[i]], use_container_width=True)
                        st.markdown("</div>", unsafe_allow_html=True)
        else:
            st.info("The selected report contains no property data.")
    else:
        st.warning("Please create or select a report to view analytics.")

# VIEW: REPORTS GRID
else:
    st.title("Market Intelligence Hub")
    if not saved.empty:
        cols = st.columns(3)
        for idx, r in saved.iterrows():
            with cols[idx % 3]:
                st.markdown(f"""
                <div class='main-card'>
                    <h4>{r['report_name']}</h4>
                    <p style='color: #64748B;'>Snapshot: {r['snapshot_date']}</p>
                    <hr style='opacity: 0.1;'>
                    <span class='badge badge-ready'>Ready</span>
                </div>
                """, unsafe_allow_html=True)
                if st.button(f"Open {r['report_name']}", key=r['import_id']):
                    st.session_state.active_report = r['import_id']
                    st.session_state.view = 'Properties'
                    st.rerun()
    else:
        st.info("Your hub is empty. Create your first report to see insights.")
