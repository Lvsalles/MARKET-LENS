# backend/ui/styles.py
import streamlit as st

def apply_premium_style():
    st.markdown("""
    <style>
    /* Global Background & Typography */
    .stApp {
        background-color: #F8FAFC;
    }
    
    /* Sidebar Styling */
    section[data-testid="stSidebar"] {
        background-color: #FFFFFF !important;
        border-right: 1px solid #E2E8F0;
        width: 280px !important;
    }

    /* Premium Cards */
    div.stActionButton button {
        border-radius: 12px;
    }
    
    .main-card {
        background-color: #FFFFFF;
        padding: 24px;
        border-radius: 16px;
        border: 1px solid #E2E8F0;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05);
        margin-bottom: 20px;
    }

    /* Status Badges */
    .badge {
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 12px;
        font-weight: 600;
    }
    .badge-ready { background-color: #DCFCE7; color: #166534; }
    .badge-setup { background-color: #FEF3C7; color: #92400E; }
    .badge-locked { background-color: #F1F5F9; color: #475569; }

    /* Action Panel (Right Side) */
    .action-panel {
        background-color: #FFFFFF;
        border-left: 1px solid #E2E8F0;
        padding: 20px;
        height: 100vh;
    }

    /* Tab Styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: transparent;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 8px 8px 0px 0px;
        padding: 8px 16px;
    }
    </style>
    """, unsafe_allow_html=True)
