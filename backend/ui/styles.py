import streamlit as st

def apply_premium_style():
    st.markdown("""
    <style>
    /* Global Canvas: Very Light Blue/White to reduce fatigue */
    .stApp {
        background-color: #F4F7FA;
    }

    /* Sidebar: Airy, Light Neutral, No Borders */
    section[data-testid="stSidebar"] {
        background-color: #F8FAFC !important;
        border-right: none !important;
        box-shadow: none !important;
        width: 280px !important;
    }

    /* Sidebar Items: Smooth Hover & Indicator */
    .stButton > button {
        border: none;
        background-color: transparent;
        color: #475569;
        text-align: left;
        padding: 10px 16px;
        border-radius: 10px;
        transition: all 0.2s ease-in-out;
        font-weight: 500;
        width: 100%;
    }

    .stButton > button:hover {
        background-color: #EEF2FF;
        color: #4F46E5;
        box-shadow: inset 4px 0px 0px #4F46E5; /* Left indicator bar */
    }

    /* Active Sidebar Item */
    div[data-testid="stSidebarNav"] .active {
        background-color: #E0E7FF !important;
        color: #4F46E5 !important;
        font-weight: 600 !important;
    }

    /* Main Floating Cards */
    .main-card {
        background-color: #FFFFFF;
        padding: 30px;
        border-radius: 16px;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.03);
        border: 1px solid rgba(226, 232, 240, 0.6);
        margin-bottom: 24px;
    }

    /* Modern Pill Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 12px;
        background-color: transparent;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 30px !important;
        padding: 8px 20px !important;
        color: #64748B;
        transition: all 0.3s;
    }
    .stTabs [aria-selected="true"] {
        background-color: #4F46E5 !important;
        color: white !important;
        border-color: #4F46E5 !important;
    }

    /* Action Panel Badges */
    .step-badge {
        padding: 4px 10px;
        border-radius: 8px;
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        font-weight: 700;
    }
    .status-ready { background-color: #DCFCE7; color: #15803d; }
    .status-setup { background-color: #FEF3C7; color: #92400e; }
    .status-locked { background-color: #F1F5F9; color: #64748b; }

    /* Hide Default Streamlit Elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    </style>
    """, unsafe_allow_html=True)
