import streamlit as st
from datetime import date
from backend.etl import run_batch_etl
from backend.core.reports import MarketReports
from backend.ui.styles import apply_premium_style

st.set_page_config(page_title="Market Lens", layout="wide")
apply_premium_style()
reports = MarketReports()

if 'view' not in st.session_state: st.session_state.view = 'Properties'

# SIDEBAR
with st.sidebar:
    st.title("Market Lens")
    if st.button("✨ New Report"): st.session_state.view = 'New Report'
    st.markdown("<hr>", unsafe_allow_html=True)
    if st.button("🏠 Properties"): st.session_state.view = 'Properties'
    if st.button("🌳 Land"): st.session_state.view = 'Land'
    if st.button("🏢 Rental"): st.session_state.view = 'Rental'
    
    st.markdown("<hr>", unsafe_allow_html=True)
    saved = reports.load_data() # Logic to list reports here
    # report_selector...

# WORKSPACE
if st.session_state.view == 'New Report':
    st.header("New Isolated Analysis")
    files = st.file_uploader("Upload MLS (Max 25)", accept_multiple_files=True)
    if files:
        report_name = st.text_input("Report Name")
        queue = []
        for f in files:
            c1, c2 = st.columns([3, 1])
            c1.write(f.name)
            t = c2.selectbox("Type", ["Properties", "Land", "Rental"], key=f"t_{f.name}")
            queue.append({"file": f, "type": t})
        if st.button("Run ETL"):
            res = run_batch_etl(queue, report_name, date.today())
            if res['ok']: st.success("Created!")

elif st.session_state.view in ['Properties', 'Land', 'Rental']:
    # Load data by session_state.active_id and category=st.session_state.view
    # Create Tabs: Overview, Compare, and Zip Codes
    st.title(f"{st.session_state.view} Workspace")
    # Dynamic tabs logic...
