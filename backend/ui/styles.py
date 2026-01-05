import streamlit as st

def apply_premium_style():
    st.markdown("""
    <style>
    .stApp { background-color: #F0F4F8; }
    section[data-testid="stSidebar"] {
        background-color: #F8FAFC !important;
        border-right: 1px solid #E2E8F0 !important;
        width: 300px !important;
    }
    .stButton > button {
        border: none; background-color: transparent; color: #475569;
        text-align: left; padding: 10px 15px; border-radius: 8px;
        transition: all 0.2s; width: 100%; font-weight: 500;
    }
    .stButton > button:hover {
        background-color: #EEF2FF; color: #4F46E5;
        box-shadow: inset 4px 0px 0px #4F46E5;
    }
    .main-card {
        background-color: #FFFFFF; padding: 20px; border-radius: 16px;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.03);
        border: 1px solid rgba(226, 232, 240, 0.8); margin-bottom: 20px;
    }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] {
        background-color: #FFFFFF; border: 1px solid #E2E8F0;
        border-radius: 20px !important; padding: 5px 15px !important;
    }
    .stTabs [aria-selected="true"] {
        background-color: #4F46E5 !important; color: white !important;
    }
    </style>
    """, unsafe_allow_html=True)
