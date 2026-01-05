import streamlit as st

def apply_premium_style():
    st.markdown("""
    <style>
    /* Global Background */
    .stApp { background-color: #F0F4F8; }
    
    /* Sidebar: Google AI Studio Style */
    section[data-testid="stSidebar"] {
        background-color: #F8FAFC !important;
        border-right: 1px solid #E2E8F0 !important;
    }
    
    /* Airy Sidebar Buttons */
    .stButton > button {
        border: none !important;
        background-color: transparent !important;
        color: #475569 !important;
        text-align: left !important;
        padding: 10px 20px !important;
        width: 100% !important;
        font-weight: 500 !important;
        transition: 200ms;
    }
    .stButton > button:hover {
        background-color: #EEF2FF !important;
        color: #4F46E5 !important;
        box-shadow: inset 4px 0px 0px #4F46E5 !important;
    }

    /* Cards */
    .main-card {
        background-color: #FFFFFF;
        padding: 24px;
        border-radius: 16px;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.03);
        border: 1px solid #E2E8F0;
        margin-bottom: 20px;
    }

    /* Pill Tabs */
    .stTabs [data-baseweb="tab-list"] { gap: 10px; }
    .stTabs [data-baseweb="tab"] {
        background-color: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 30px !important;
        padding: 8px 20px !important;
    }
    .stTabs [aria-selected="true"] {
        background-color: #4F46E5 !important;
        color: white !important;
    }
    
    .nav-divider { height: 1px; background: #E2E8F0; margin: 15px 10px; opacity: 0.5; }
    </style>
    """, unsafe_allow_html=True)
