import streamlit as st
import pandas as pd
import uuid

from db import engine
from normalization import normalize_columns, enforce_schema

st.set_page_config(page_title="Market Lens – Upload Center", layout="wide")
st.title("📊 Market Lens — Upload Center")

uploaded_files = st.file_uploader(
    "Upload CSV/XLSX files",
    type=["csv", "xlsx"],
    accept_multiple_files=True
)

if st.button("Process & Save") and uploaded_files:
    upload_id = str(uuid.uuid4())
    st.write("Upload ID:", upload_id)

    for file in uploaded_files:
        try:
            if file.name.endswith(".csv"):
                df = pd.read_csv(file)
            else:
                df = pd.read_excel(file)

            df["upload_id"] = upload_id

            df = normalize_columns(df)
            df = enforce_schema(df, "residential")

            df.to_sql(
                "residential_listings",
                engine,
                if_exists="append",
                index=False
            )

            st.success(f"{file.name}: uploaded {len(df)} rows")

        except Exception as e:
            st.error(f"{file.name}: {e}")
