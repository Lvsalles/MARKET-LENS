import streamlit as st

def apply_premium_style():
    st.markdown("""
    <style>
    /* Global Canvas */
    .stApp { background-color: #F0F4F8; }
    
    /* Sidebar: Airy, Light Neutral, No Borders */
    section[data-testid="stSidebar"] {
        background-color: #F8FAFC !important;
        border-right: 1px solid #E2E8F0 !important;
        width: 320px !important;
    }
    
    /* Sidebar Buttons: borderless with left indicator */
    .stButton > button {
        border: none !important;
        background-color: transparent !important;
        color: #475569 !important;
        text-align: left !important;
        padding: 12px 20px !important;
        border-radius: 10px !important;
        transition: all 200ms ease-in-out !important;
        width: 100% !important;
        font-weight: 500 !important;
    }
    .stButton > button:hover {
        background-color: #EEF2FF !important;
        color: #4F46E5 !important;
        box-shadow: inset 4px 0px 0px #4F46E5 !important;
    }

    /* Premium Floating Cards */
    .main-card {
        background-color: #FFFFFF;
        padding: 24px;
        border-radius: 16px;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.03);
        border: 1px solid rgba(226, 232, 240, 0.8);
        margin-bottom: 20px;
    }

    /* Modern Pill Tabs */
    .stTabs [data-baseweb="tab-list"] { gap: 10px; }
    .stTabs [data-baseweb="tab"] {
        background-color: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 30px !important;
        padding: 8px 20px !important;
        color: #64748B;
    }
    .stTabs [aria-selected="true"] {
        background-color: #4F46E5 !important;
        color: white !important;
    }
    
    /* Thin Divider */
    .nav-divider { height: 1px; background: #E2E8F0; margin: 20px 15px; opacity: 0.5; }
    </style>
    """, unsafe_allow_html=True)
