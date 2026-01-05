import streamlit as st

def apply_premium_style():
    st.markdown("""
    <style>
    /* Global Canvas: Light Airy Blue */
    .stApp { background-color: #F8FAFF; }
    
    /* Sidebar: Neutral, No Borders, Floating feel */
    section[data-testid="stSidebar"] {
        background-color: #FFFFFF !important;
        border-right: 1px solid #E6E9EF !important;
        width: 320px !important;
    }
    
    /* Sidebar Buttons: Icon + Text style */
    .stButton > button {
        border: none;
        background-color: transparent;
        color: #475569;
        text-align: left;
        padding: 12px 20px;
        border-radius: 12px;
        transition: all 0.2s ease;
        width: 100%;
        font-weight: 500;
    }
    .stButton > button:hover {
        background-color: #F0F4FF;
        color: #4F46E5;
        box-shadow: inset 4px 0px 0px #4F46E5;
    }

    /* Main Content Cards */
    .main-card {
        background-color: #FFFFFF;
        padding: 24px;
        border-radius: 16px;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.04);
        border: 1px solid #EDF2F7;
        margin-bottom: 20px;
    }

    /* Modern Pill Tabs */
    .stTabs [data-baseweb="tab-list"] { gap: 12px; }
    .stTabs [data-baseweb="tab"] {
        background-color: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 30px !important;
        padding: 8px 24px !important;
        color: #64748B;
    }
    .stTabs [aria-selected="true"] {
        background-color: #4F46E5 !important;
        color: white !important;
        box-shadow: 0 4px 12px rgba(79, 70, 229, 0.3);
    }
    
    /* Separator */
    .thin-sep { height: 1px; background: #E2E8F0; margin: 20px 10px; }
    </style>
    """, unsafe_allow_html=True)
