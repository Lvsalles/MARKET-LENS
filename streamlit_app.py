import streamlit as st

# 1. CRITICAL: MUST BE THE FIRST LINE
st.set_page_config(page_title="Market Lens", layout="wide")

from datetime import date
from backend.etl import run_batch_etl
from backend.core.reports import MarketReports
from backend.ui.styles import apply_premium_style

# 2. Apply Style with fallback
try:
    apply_premium_style()
except:
    st.warning("Style engine failed to load, using default.")

# 3. Initialize
reports = MarketReports()
if 'view' not in st.session_state: st.session_state.view = 'Properties'
if 'active_report_id' not in st.session_state: st.session_state.active_report_id = None

# --- SIDEBAR ---
with st.sidebar:
    st.markdown("<h2 style='color:#1E293B;'>Market Lens</h2>", unsafe_allow_html=True)
    if st.button("✨ New Report"): st.session_state.view = 'New Report'
    st.markdown("<div class='nav-divider'></div>", unsafe_allow_html=True)
    
    st.markdown("<b>ANALYTICS</b>", unsafe_allow_html=True)
    if st.button("🏠 Properties"): st.session_state.view = 'Properties'
    if st.button("🌳 Land"): st.session_state.view = 'Land'
    if st.button("🏢 Rental"): st.session_state.view = 'Rental'

    st.markdown("<div class='nav-divider'></div>", unsafe_allow_html=True)
    
    st.subheader("Active Report")
    try:
        saved = reports.list_all_reports()
        if not saved.empty:
            opts = {f"{r['report_name']}": r['import_id'] for _, r in saved.iterrows()}
            sel = st.selectbox("Select Report:", options=list(opts.keys()), label_visibility="collapsed")
            st.session_state.active_report_id = opts[sel]
    except:
        st.caption("No reports available.")

# --- WORKSPACE ---
if st.session_state.view == 'New Report':
    st.title("Data Ingestion")
    st.markdown("<div class='main-card'>", unsafe_allow_html=True)
    files = st.file_uploader("Upload MLS Files (Max 25)", accept_multiple_files=True, type=['csv', 'xlsx'])
    if files:
        report_name = st.text_input("Report Name")
        files_data = []
        for f in files:
            c1, c2 = st.columns([3, 1])
            c1.write(f.name)
            t = c2.selectbox("Type", ["Properties", "Land", "Rental"], key=f"t_{f.name}")
            files_data.append({'file': f, 'type': t})
        if st.button("🚀 Run ETL"):
            res = run_batch_etl(files_data, report_name, date.today())
            if res['ok']: 
                st.session_state.active_report_id = res['import_id']
                st.session_state.view = 'Properties'
                st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)

elif st.session_state.view in ['Properties', 'Land', 'Rental']:
    st.title(f"{st.session_state.view} Analytics")
    if st.session_state.active_report_id:
        df = reports.load_report_data(st.session_state.active_report_id, st.session_state.view)
        if not df.empty:
            zips = sorted([str(z) for z in df['zip'].unique() if z])
            tabs = st.tabs(["📊 Overview", "⚖️ Compare"] + [f"📍 {z}" for z in zips])
            
            with tabs[0]:
                st.markdown("<div class='main-card'>", unsafe_allow_html=True)
                st.dataframe(reports.get_inventory_overview(df), use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)
        else:
            st.info("No data found for this category.")
    else:
        st.warning("Please select a report in the sidebar.")
