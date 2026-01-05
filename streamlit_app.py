import streamlit as st
import pandas as pd
from datetime import date
from backend.etl import run_etl_batch
from backend.core.reports import MarketReports
from backend.ui.styles import apply_premium_style

# 1. Page Configuration
st.set_page_config(page_title="Market Lens AI", layout="wide", initial_sidebar_state="expanded")
apply_premium_style()

# 2. State Management for Navigation
if 'active_report_id' not in st.session_state: st.session_state.active_report_id = None
if 'view' not in st.session_state: st.session_state.view = 'Properties'

reports = MarketReports()

# 3. Sidebar (Left Navigation)
with st.sidebar:
    st.markdown("<h1 style='font-size: 22px; color: #1E293B; padding-left: 10px;'>Market Lens</h1>", unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    # Core Navigation
    if st.button("➕ New Report", use_container_width=True): st.session_state.view = 'New Report'
    if st.button("📑 All Reports", use_container_width=True): st.session_state.view = 'Reports'
    
    # Thin Divider
    st.markdown("<div class='thin-divider'></div>", unsafe_allow_html=True)
    
    # Categories
    st.markdown("<p style='font-size: 11px; font-weight: 700; color: #94A3B8; padding-left: 15px; letter-spacing: 1px;'>ANALYTICS</p>", unsafe_allow_html=True)
    
    if st.button("🏠 Properties", use_container_width=True): st.session_state.view = 'Properties'
    if st.button("🌳 Land", use_container_width=True): st.session_state.view = 'Land'
    if st.button("🏢 Rental", use_container_width=True): st.session_state.view = 'Rental'

    st.markdown("<div class='thin-divider'></div>", unsafe_allow_html=True)
    
    # Isolated Report Selector
    st.subheader("Active Analysis")
    try:
        saved_reports = reports.list_all_reports()
        if not saved_reports.empty:
            report_map = {f"{r['report_name']}": r['import_id'] for _, r in saved_reports.iterrows()}
            selected_name = st.selectbox("Select Dataset:", options=list(report_map.keys()), label_visibility="collapsed")
            st.session_state.active_report_id = report_map[selected_name]
        else:
            st.caption("No reports available.")
    except:
        st.error("Database connection error.")

# 4. Main Content Area Logic
view = st.session_state.view

if view == 'New Report':
    st.title("Create New Isolated Report")
    st.markdown("<div class='main-card'>", unsafe_allow_html=True)
    # ... (ETL code from previous batch upload version goes here)
    st.markdown("</div>", unsafe_allow_html=True)

elif view in ['Properties', 'Land', 'Rental']:
    if st.session_state.active_report_id:
        # LOAD ISOLATED DATA
        df = reports.load_report_data(st.session_state.active_report_id)
        
        # Filter by category if needed
        # df = df[df['asset_class'] == view]

        st.title(f"{view} Intelligence")
        
        if not df.empty:
            # DYNAMIC TOP TABS
            unique_zips = sorted([str(z) for z in df['zip'].unique() if z])
            tab_labels = ["📍 ZIP Overview", "⚖️ ZIP Compare"] + unique_zips
            tabs = st.tabs(tab_labels)

            # Tab 1: ZIP Code Overview
            with tabs[0]:
                st.markdown("<div class='main-card'>", unsafe_allow_html=True)
                st.subheader("Global Market Inventory")
                st.dataframe(reports.get_inventory_overview(df), use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)

            # Tab 2: ZIP Code Compare
            with tabs[1]:
                st.markdown("<div class='main-card'>", unsafe_allow_html=True)
                st.subheader("Cross-Region Analysis")
                st.info("Select multiple ZIP codes to compare performance metrics.")
                # Logic for comparison table
                st.markdown("</div>", unsafe_allow_html=True)

            # Dynamic Tabs for each ZIP
            for i, zip_code in enumerate(unique_zips):
                with tabs[i+2]:
                    st.markdown("<div class='main-card'>", unsafe_allow_html=True)
                    st.subheader(f"Detailed Analysis for {zip_code}")
                    zip_data = df[df['zip'].astype(str) == zip_code]
                    
                    k1, k2, k3 = st.columns(3)
                    k1.metric("Listings", len(zip_data))
                    k2.metric("Avg Price", f"${zip_data['list_price'].mean():,.0f}")
                    k3.metric("Avg SQFT", f"{zip_data['heated_area'].mean():,.0f}")
                    
                    st.dataframe(zip_data, use_container_width=True)
                    st.markdown("</div>", unsafe_allow_html=True)
        else:
            st.warning(f"No data found for {view} in the selected report.")
    else:
        st.info("Please select a report from the sidebar to view analytics.")

else:
    st.title("Saved Intelligence Reports")
    # Grid of reports...
