import streamlit as st

def apply_premium_style():
    st.markdown("""
    <style>
    /* Global Canvas: Light Airy Blue */
    .stApp { background-color: #F0F4F8; }
    
    /* Sidebar: Neutral, No Borders, Airy */
    section[data-testid="stSidebar"] {
        background-color: #F8FAFC !important;
        border-right: 1px solid #E2E8F0 !important;
        width: 320px !important;
    }
    
    /* Sidebar Buttons: Icon + Text style with Indicator */
    .stButton > button {
        border: none;
        background-color: transparent;
        color: #475569;
        text-align: left;
        padding: 12px 20px;
        border-radius: 10px;
        transition: all 200ms ease-in-out;
        width: 100%;
        font-weight: 500;
    }
    .stButton > button:hover {
        background-color: #EEF2FF;
        color: #4F46E5;
        box-shadow: inset 4px 0px 0px #4F46E5;
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
        font-size: 14px;
    }
    .stTabs [aria-selected="true"] {
        background-color: #4F46E5 !important;
        color: white !important;
        box-shadow: 0 4px 12px rgba(79, 70, 229, 0.3);
    }
    
    /* Separator Line */
    .nav-divider { height: 1px; background: #E2E8F0; margin: 20px 15px; opacity: 0.5; }
    </style>
    """, unsafe_allow_html=True)
