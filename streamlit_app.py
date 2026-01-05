import streamlit as st
from datetime import date
from backend.etl import run_batch_etl
from backend.core.reports import MarketReports
from backend.ui.styles import apply_premium_style

st.set_page_config(page_title="Market Lens Enterprise", layout="wide")
apply_premium_style()
reports = MarketReports()

# State Management
if 'view' not in st.session_state: st.session_state.view = 'Properties'
if 'active_report_id' not in st.session_state: st.session_state.active_report_id = None

# --- SIDEBAR NAVIGATION ---
with st.sidebar:
    st.markdown("<h2 style='color:#1E293B; margin-left:10px;'>Market Lens</h2>", unsafe_allow_html=True)
    if st.button("✨ New Report", use_container_width=True): st.session_state.view = 'New Report'
    st.markdown("<div class='nav-divider'></div>", unsafe_allow_html=True)
    
    st.markdown("<p style='font-size:11px; font-weight:700; color:#94A3B8; margin-left:15px;'>ANALYTICS</p>", unsafe_allow_html=True)
    if st.button("🏠 Properties", use_container_width=True): st.session_state.view = 'Properties'
    if st.button("🌳 Land", use_container_width=True): st.session_state.view = 'Land'
    if st.button("🏢 Rental", use_container_width=True): st.session_state.view = 'Rental'

    st.markdown("<div class='nav-divider'></div>", unsafe_allow_html=True)
    
    # Report Selector Logic
    st.subheader("Active Analysis")
    try:
        saved = reports.list_all_reports()
        if not saved.empty:
            opts = {f"{r['report_name']} ({r['snapshot_date']})": r['import_id'] for _, r in saved.iterrows()}
            sel = st.selectbox("Current Dataset:", options=list(opts.keys()), label_visibility="collapsed")
            st.session_state.active_report_id = opts[sel]
        else:
            st.caption("No reports available.")
    except:
        st.error("Database error.")

# --- WORKSPACE ROUTING ---
view = st.session_state.view

if view == 'New Report':
    st.title("Market Intelligence Ingestion")
    st.markdown("<div class='main-card'>", unsafe_allow_html=True)
    files = st.file_uploader("Upload MLS Files (Max 25)", accept_multiple_files=True, type=['csv', 'xlsx'])
    
    if files:
        report_name = st.text_input("Report Name", placeholder="e.g. North Port Q1 2026")
        st.markdown("### 📋 File Classification Queue")
        files_to_process = []
        for f in files:
            c1, c2 = st.columns([3, 1])
            c1.markdown(f"**📄 {f.name}**")
            f_type = c2.selectbox("Type", ["Properties", "Land", "Rental"], key=f"t_{f.name}")
            files_to_process.append({'file': f, 'type': f_type})
        
        if st.button("🚀 Run Batch ETL", type="primary", use_container_width=True):
            if report_name:
                with st.spinner("Processing..."):
                    res = run_batch_etl(files_to_process, report_name, date.today())
                    if res['ok']:
                        st.success("Success!")
                        st.session_state.active_report_id = res['import_id']
                        st.session_state.view = 'Properties'
                        st.rerun()
            else: st.warning("Please name your report.")
    st.markdown("</div>", unsafe_allow_html=True)

elif view in ['Properties', 'Land', 'Rental']:
    if st.session_state.active_report_id:
        df = reports.load_report_data(st.session_state.active_report_id, view)
        st.title(f"{view} Workspace")
        
        if not df.empty:
            zips = sorted([str(z) for z in df['zip'].unique() if z])
            tab_labels = ["📊 Overview", "⚖️ Compare"] + [f"📍 {z}" for z in zips]
            tabs = st.tabs(tab_labels)
            
            with tabs[0]:
                st.markdown("<div class='main-card'>", unsafe_allow_html=True)
                st.dataframe(reports.get_inventory_overview(df), use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)
            
            with tabs[1]:
                st.markdown("<div class='main-card'>", unsafe_allow_html=True)
                st.subheader("ZIP Code Comparison")
                st.write("Comparison metrics logic goes here.")
                st.markdown("</div>", unsafe_allow_html=True)

            for i, zip_code in enumerate(zips):
                with tabs[i+2]:
                    st.markdown("<div class='main-card'>", unsafe_allow_html=True)
                    zip_df = df[df['zip'].astype(str) == zip_code]
                    st.metric("Listings", len(zip_df))
                    st.dataframe(zip_df, use_container_width=True)
                    st.markdown("</div>", unsafe_allow_html=True)
        else:
            st.info(f"No {view} data in this report.")
    else:
        st.warning("Select or create a report first.")
