import streamlit as st

def apply_premium_style():
    st.markdown("""
    <style>
    /* Global Canvas - Airy Light Blue/Grey */
    .stApp { background-color: #F4F7FB; }
    
    /* Sidebar - Clean White, Borderless */
    section[data-testid="stSidebar"] {
        background-color: #FFFFFF !important;
        border-right: 1px solid #E2E8F0 !important;
        width: 300px !important;
    }
    
    /* Sidebar Navigation Buttons */
    .stButton > button {
        border: none !important;
        background-color: transparent !important;
        color: #64748B !important;
        text-align: left !important;
        padding: 12px 16px !important;
        width: 100% !important;
        font-weight: 500 !important;
        transition: 200ms ease;
    }
    
    .stButton > button:hover {
        background-color: #F8FAFF !important;
        color: #4F46E5 !important;
        box-shadow: inset 4px 0px 0px #4F46E5 !important;
    }

    /* Floating Cards */
    .main-card {
        background-color: #FFFFFF;
        padding: 24px;
        border-radius: 12px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        border: 1px solid #E2E8F0;
        margin-bottom: 20px;
    }

    /* Pill Tabs */
    .stTabs [data-baseweb="tab-list"] { gap: 10px; }
    .stTabs [data-baseweb="tab"] {
        background-color: #FFFFFF;
        border-radius: 20px !important;
        padding: 6px 20px !important;
        border: 1px solid #E2E8F0;
    }
    .stTabs [aria-selected="true"] {
        background-color: #4F46E5 !important;
        color: white !important;
    }
    
    /* Elegant Divider */
    .nav-sep { height: 1px; background: #E2E8F0; margin: 15px 0; opacity: 0.5; }
    </style>
    """, unsafe_allow_html=True)
