import streamlit as st

# 1. MUST BE FIRST
st.set_page_config(page_title="Market Lens", layout="wide", initial_sidebar_state="expanded")

from datetime import date
from backend.etl import run_batch_etl
from backend.core.reports import MarketReports
from backend.ui.styles import apply_premium_style

apply_premium_style()
reports = MarketReports()

# State Management
if 'view' not in st.session_state: st.session_state.view = 'Properties'
if 'active_id' not in st.session_state: st.session_state.active_id = None

# --- SIDEBAR ---
with st.sidebar:
    st.markdown("<h2 style='color:#1E293B;'>Market Lens</h2>", unsafe_allow_html=True)
    if st.button("✨ New Report"): st.session_state.view = 'New Report'
    st.markdown("<div class='nav-sep'></div>", unsafe_allow_html=True)
    
    st.caption("ANALYTICS")
    if st.button("🏠 Properties"): st.session_state.view = 'Properties'
    if st.button("🌳 Land"): st.session_state.view = 'Land'
    if st.button("🏢 Rental"): st.session_state.view = 'Rental'
    
    st.markdown("<div class='nav-sep'></div>", unsafe_allow_html=True)
    
    st.caption("ACTIVE REPORT")
    try:
        saved = reports.list_reports()
        if not saved.empty:
            opts = {f"{r['report_name']}": r['import_id'] for _, r in saved.iterrows()}
            sel = st.selectbox("Switch:", options=list(opts.keys()), label_visibility="collapsed")
            st.session_state.active_id = opts[sel]
    except:
        st.error("Database connection error.")

# --- WORKSPACE ---
view = st.session_state.view

if view == 'New Report':
    st.title("Data Ingestion")
    st.markdown("<div class='main-card'>", unsafe_allow_html=True)
    files = st.file_uploader("Upload up to 25 files", accept_multiple_files=True, type=['csv', 'xlsx'])
    if files:
        report_name = st.text_input("Analysis Name", placeholder="e.g. North Port 2025")
        queue = []
        for f in files:
            c1, c2 = st.columns([3, 1])
            c1.markdown(f"**📄 {f.name}**")
            t = c2.selectbox("Type", ["Properties", "Land", "Rental"], key=f"type_{f.name}")
            queue.append({'file': f, 'type': t})
        
        if st.button("🚀 Run Batch ETL"):
            if report_name:
                with st.spinner("Processing..."):
                    res = run_batch_etl(queue, report_name, date.today())
                    if res['ok']:
                        st.session_state.active_id = res['import_id']
                        st.session_state.view = 'Properties'
                        st.rerun()
            else: st.warning("Name the report.")
    st.markdown("</div>", unsafe_allow_html=True)

elif view in ['Properties', 'Land', 'Rental']:
    st.title(f"{view} Analytics")
    if st.session_state.active_id:
        df = reports.load_data(st.session_state.active_id, view)
        if not df.empty:
            zips = sorted([str(z) for z in df['zip'].unique() if z])
            tabs = st.tabs(["📊 Overview", "⚖️ Compare"] + [f"📍 {z}" for z in zips])
            
            with tabs[0]:
                st.markdown("<div class='main-card'>", unsafe_allow_html=True)
                st.dataframe(reports.get_summary(df), use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)
            
            for i, zip_code in enumerate(zips):
                with tabs[i+2]:
                    st.markdown("<div class='main-card'>", unsafe_allow_html=True)
                    st.dataframe(df[df['zip'].astype(str) == zip_code], use_container_width=True)
                    st.markdown("</div>", unsafe_allow_html=True)
        else:
            st.info(f"No {view} data found in this report.")
    else:
        st.warning("Please select a report in the sidebar.")
