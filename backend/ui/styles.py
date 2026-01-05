import streamlit as st

def apply_premium_style():
    st.markdown("""
    <style>
    /* Global Canvas - Very Light Professional Blue */
    .stApp {
        background-color: #F0F4F8;
    }

    /* Sidebar - Airy, Light Neutral, No Borders */
    section[data-testid="stSidebar"] {
        background-color: #F8FAFC !important;
        border-right: 1px solid #E2E8F0;
        width: 300px !important;
    }

    /* Sidebar Navigation Items */
    .nav-item {
        display: flex;
        align-items: center;
        padding: 12px 16px;
        margin: 4px 12px;
        border-radius: 8px;
        color: #475569;
        text-decoration: none;
        transition: all 0.2s ease-in-out;
        cursor: pointer;
        font-weight: 500;
        border-left: 4px solid transparent;
    }

    .nav-item:hover {
        background-color: #EEF2FF;
        color: #4F46E5;
        border-left: 4px solid #4F46E5;
    }

    .nav-active {
        background-color: #E0E7FF;
        color: #4F46E5;
        font-weight: 600;
        border-left: 4px solid #4F46E5;
    }

    /* Sidebar Status Badges */
    .status-dot {
        height: 8px;
        width: 8px;
        border-radius: 50%;
        display: inline-block;
        margin-right: 10px;
    }
    .ready { background-color: #22C55E; }
    .setup { background-color: #F59E0B; }
    .locked { background-color: #94A3B8; }

    /* Floating Content Cards */
    .main-card {
        background-color: #FFFFFF;
        padding: 25px;
        border-radius: 16px;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.03);
        border: 1px solid rgba(226, 232, 240, 0.8);
        margin-bottom: 20px;
    }

    /* Modern Pill Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 10px;
        background-color: transparent;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 25px !important;
        padding: 6px 18px !important;
        color: #64748B;
        font-size: 14px;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    }
    .stTabs [aria-selected="true"] {
        background-color: #4F46E5 !important;
        color: white !important;
        border-color: #4F46E5 !important;
        box-shadow: 0 4px 10px rgba(79, 70, 229, 0.3);
    }

    /* Thin Divider */
    .thin-divider {
        height: 1px;
        background-color: #E2E8F0;
        margin: 20px 15px;
        opacity: 0.6;
    }

    /* Metrics Styling */
    [data-testid="stMetricValue"] {
        font-size: 28px;
        font-weight: 700;
        color: #1E293B;
    }
    </style>
    """, unsafe_allow_html=True)
