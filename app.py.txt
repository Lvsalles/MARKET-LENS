import streamlit as st
import pandas as pd

st.title("Market Analysis Webtool")
st.write("Hello! This is your AI tool running on the cloud.")

# Upload button
uploaded_file = st.file_uploader("Choose your CSV file", type="csv")

if uploaded_file:
    df = pd.read_csv(uploaded_file)
    st.write("### Data loaded successfully!")
    st.dataframe(df.head(10))