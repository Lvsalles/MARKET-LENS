import streamlit as st
import pandas as pd
from datetime import date
from backend.etl import run_etl_batch
from backend.core.reports import MarketReports
from backend.ui.styles import apply_premium_style

# 1. Page Configuration & Style
st.set_page_config(page_title="Market Lens Enterprise", layout="wide", initial_sidebar_state="expanded")
apply_premium_style()

# Initialize API classes
reports = MarketReports()

# Navigation State
if 'view' not in st.session_state: st.session_state.view = 'Properties'
if 'active_report_id' not in st.session_state: st.session_state.active_report_id = None

# --- SIDEBAR NAVIGATION (Premium Style) ---
with st.sidebar:
    st.markdown("<h1 style='font-size: 26px; color: #1E293B; margin-bottom: -10px;'>Market Lens</h1>", unsafe_allow_html=True)
    st.markdown("<p style='color: #64748B; font-size: 12px;'>Real Estate Intelligence v1.0</p>", unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    if st.button("✨ New Report", use_container_width=True): 
        st.session_state.view = 'New Report'
    
    if st.button("📑 Reports Hub", use_container_width=True): 
        st.session_state.view = 'Reports'
    
    # Thin Elegant Separator
    st.markdown("<hr style='margin: 15px 10px; opacity: 0.3; border-top: 1px solid #94A3B8;'>", unsafe_allow_html=True)
    
    st.markdown("<p style='font-size: 11px; font-weight: 700; color: #94A3B8; letter-spacing: 1.5px; margin-left: 10px;'>ANALYTICS</p>", unsafe_allow_html=True)
    
    # Navigation Categories
    if st.button("🏠 Properties", use_container_width=True): st.session_state.view = 'Properties'
    if st.button("🌳 Land", use_container_width=True): st.session_state.view = 'Land'
    if st.button("🏢 Rental", use_container_width=True): st.session_state.view = 'Rental'

    st.markdown("<br><br>", unsafe_allow_html=True)
    
    # Isolated Report Selector
    st.subheader("Active Workspace")
    try:
        saved = reports.list_all_reports()
        if not saved.empty:
            report_options = {f"{r['report_name']}": r['import_id'] for _, r in saved.iterrows()}
            selected_label = st.selectbox("Switch Report:", options=list(report_options.keys()), label_visibility="collapsed")
            st.session_state.active_report_id = report_options[selected_label]
        else:
            st.caption("No reports available. Create one to start.")
    except:
        st.error("Database connection error.")

# --- MAIN CONTENT AREA ---
view = st.session_state.view

# VIEW: NEW REPORT
if view == 'New Report':
    st.title("Create New Intelligence Report")
    st.markdown("<div class='main-card'>", unsafe_allow_html=True)
    
    files = st.file_uploader("Upload MLS Files (Max 25)", accept_multiple_files=True, type=['csv', 'xlsx'])
    
    if files:
        report_name = st.text_input("Report Name", placeholder="e.g. North Port Q1 2026")
        st.markdown("### 📋 File Classification")
        files_data = []
        for f in files:
            c1, c2 = st.columns([3, 1])
            c1.markdown(f"**📄 {f.name}**")
            f_type = c2.selectbox("Type", ["Properties", "Land", "Rental"], key=f"type_{f.name}", label_visibility="collapsed")
            files_data.append({'file': f, 'type': f_type})
        
        if st.button("🚀 Process Batch & Generate Report", type="primary", use_container_width=True):
            if report_name:
                with st.spinner("Executing isolated ETL..."):
                    res = run_etl_batch(files_data=files_data, report_name=report_name, snapshot_date=date.today(), contract_path="backend/contract/mls_column_contract.yaml")
                    if res.ok:
                        st.success("Report Generated!")
                        st.rerun()
            else:
                st.warning("Please name the report.")
    st.markdown("</div>", unsafe_allow_html=True)

# VIEW: ANALYTICS MODULES (Properties / Land / Rental)
elif view in ['Properties', 'Land', 'Rental']:
    if st.session_state.active_report_id:
        # Strict Isolation: ID + Category
        df = reports.load_report_data(st.session_state.active_report_id, view)
        
        st.title(f"{view} Analytics")
        
        if not df.empty:
            # DYNAMIC TOP TABS: Overview, Comparison, and specific ZIP Codes
            unique_zips = sorted([str(z) for z in df['zip'].unique() if z])
            tab_labels = ["📊 Overview", "⚖️ Compare"] + [f"📍 {z}" for z in unique_zips]
            tabs = st.tabs(tab_labels)

            with tabs[0]: # Overview
                st.markdown("<div class='main-card'>", unsafe_allow_html=True)
                st.subheader("Market Summary")
                st.dataframe(reports.get_inventory_overview(df), use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)

            with tabs[1]: # Compare
                st.markdown("<div class='main-card'>", unsafe_allow_html=True)
                st.subheader("ZIP Code Comparison Matrix")
                st.dataframe(reports.get_comparison_matrix(df), use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)

            # Generate individual ZIP tabs
            for i, zip_code in enumerate(unique_zips):
                with tabs[i+2]:
                    st.markdown("<div class='main-card'>", unsafe_allow_html=True)
                    zip_df = df[df['zip'].astype(str) == zip_code]
                    
                    k1, k2, k3 = st.columns(3)
                    k1.metric("Listings", len(zip_df))
                    k2.metric("Avg Price", f"${zip_df['list_price'].mean():,.0f}")
                    k3.metric("Avg $/SqFt", f"${zip_df['lp_sqft'].mean():,.2f}")
                    
                    st.dataframe(zip_df, use_container_width=True)
                    st.markdown("</div>", unsafe_allow_html=True)
        else:
            st.info(f"No {view} data found for this specific report.")
    else:
        st.warning("Please select a report in the sidebar to view analytics.")

# VIEW: REPORTS HUB
else:
    st.title("Intelligence Hub")
    if not saved.empty:
        cols = st.columns(3)
        for idx, r in saved.iterrows():
            with cols[idx % 3]:
                st.markdown(f"""
                <div class='main-card'>
                    <h3 style='margin-bottom: 5px;'>{r['report_name']}</h3>
                    <p style='color: #64748B; font-size: 14px;'>Date: {r['snapshot_date']}</p>
                </div>
                """, unsafe_allow_html=True)
                if st.button(f"Open Workspace", key=f"btn_{r['import_id']}", use_container_width=True):
                    st.session_state.active_report_id = r['import_id']
                    st.session_state.view = 'Properties'
                    st.rerun()
    else:
        st.info("No reports found.")
