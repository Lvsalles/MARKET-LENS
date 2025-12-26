import streamlit as st
import pandas as pd
from db import get_engine
from normalization import normalize_columns

st.set_page_config(page_title="Market Lens – Upload Center", layout="wide")

st.title("📊 Market Lens — Upload Center")
st.caption("Upload CSV/XLSX files and persist normalized data into Supabase")

uploaded_files = st.file_uploader(
    "Upload files",
    type=["csv", "xlsx"],
    accept_multiple_files=True
)

engine = get_engine()

if uploaded_files:
    for file in uploaded_files:
        st.write(f"📄 Processing {file.name}")

        if file.name.endswith(".csv"):
            df = pd.read_csv(file)
        else:
            df = pd.read_excel(file)

        df = normalize_columns(df)

        table_name = "residential_listings"
        df.to_sql(
            table_name,
            engine,
            if_exists="append",
            index=False,
            method="multi"
        )

        st.success(f"✔ Uploaded {len(df)} rows to {table_name}")
