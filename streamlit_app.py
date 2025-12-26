import os
from datetime import datetime

import streamlit as st
import pandas as pd
import numpy as np
from pypdf import PdfReader
from docx import Document

from db import get_db_conn, insert_upload, insert_document_text, bulk_insert_dicts

st.set_page_config(page_title="MARKET LENS", layout="wide")

st.title("MARKET LENS — Upload Center")
st.caption("Upload CSV/XLSX for structured data, and PDF/DOCX for document storage + text extraction.")

# ----------------------------
# 0) Local storage folder
# ----------------------------
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

def save_uploaded_file(uploaded_file) -> str:
    """
    Saves the uploaded file to local disk and returns stored_path.
    Streamlit Cloud has an ephemeral filesystem, but this is OK for tracking + debugging.
    Next step: replace this with Supabase Storage / S3.
    """
    safe_name = uploaded_file.name.replace("/", "_").replace("\\", "_")
    stored_path = os.path.join(UPLOAD_DIR, f"{int(datetime.utcnow().timestamp())}_{safe_name}")
    with open(stored_path, "wb") as out:
        out.write(uploaded_file.getbuffer())
    return stored_path

def filetype(filename: str) -> str:
    fn = filename.lower()
    if fn.endswith(".csv"): return "csv"
    if fn.endswith(".xlsx") or fn.endswith(".xls"): return "xlsx"
    if fn.endswith(".pdf"): return "pdf"
    if fn.endswith(".docx"): return "docx"
    return "other"

def guess_dataset_type(filename: str) -> str:
    fn = filename.lower()
    if fn.endswith(".pdf") or fn.endswith(".docx"):
        return "document"
    if "land" in fn:
        return "land"
    if "agent" in fn:
        return "agent"
    return "residential"

# ----------------------------
# 1) Synonyms Library (starter)
# ----------------------------
SYNONYMS = {
    "ml_number": ["ML Number", "MLS#", "MLS Number", "Listing ID", "ListingNumber", "ml_number"],
    "status": ["Status", "MlsStatus", "Listing Status", "status"],
    "address": ["Address", "Full Address", "Street Address", "UnparsedAddress", "address"],
    "city": ["City", "city"],
    "county": ["County", "CountyOrParish", "county"],
    "zip": ["Zip", "Zip Code", "PostalCode", "Postal Code", "zip"],
    "price": ["List Price", "Price", "Current Price", "Current Price_num", "price"],
    "sqft": ["Heated Area", "Living Area", "SqFt", "sqft", "Heated Area_num"],
    "beds": ["Beds", "Bedrooms", "Beds_num", "beds"],
    "baths": ["Baths", "Bathrooms", "Full Baths", "Full Baths_num", "baths"],
    "year_built": ["Year Built", "YearBuilt", "year_built"],
}

REQUIRED_RESIDENTIAL = ["ml_number", "status", "address", "city", "county", "zip", "price", "sqft", "beds", "baths", "year_built"]

def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    return df

def dedupe_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[:, ~df.columns.duplicated()].copy()

def apply_synonyms(df: pd.DataFrame) -> pd.DataFrame:
    colmap = {}
    cols = set(df.columns)
    for canonical, candidates in SYNONYMS.items():
        found = next((c for c in candidates if c in cols), None)
        if found:
            colmap[found] = canonical
    if colmap:
        df = df.rename(columns=colmap)
    return df

def coerce_types_residential(df: pd.DataFrame) -> pd.DataFrame:
    for c in REQUIRED_RESIDENTIAL:
        if c not in df.columns:
            df[c] = np.nan

    df["price"] = pd.to_numeric(df["price"].astype(str).str.replace(r"[$,]", "", regex=True), errors="coerce")
    df["sqft"] = pd.to_numeric(df["sqft"], errors="coerce")
    df["beds"] = pd.to_numeric(df["beds"], errors="coerce")
    df["baths"] = pd.to_numeric(df["baths"], errors="coerce")
    df["year_built"] = pd.to_numeric(df["year_built"], errors="coerce")

    for c in ["ml_number", "status", "address", "city", "county", "zip"]:
        df[c] = df[c].astype(str).replace({"nan": None, "None": None})

    return df

def robust_prepare(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_headers(df)
    df = dedupe_columns(df)
    df = apply_synonyms(df)
    return df

def read_table_file(uploaded_file) -> pd.DataFrame:
    name = uploaded_file.name.lower()
    if name.endswith(".csv"):
        return pd.read_csv(uploaded_file)
    if name.endswith(".xlsx") or name.endswith(".xls"):
        return pd.read_excel(uploaded_file)
    raise ValueError("Unsupported table file. Use CSV/XLSX.")

def extract_pdf_text(file_path: str) -> str:
    reader = PdfReader(file_path)
    parts = []
    for page in reader.pages:
        parts.append(page.extract_text() or "")
    return "\n".join(parts).strip()

def extract_docx_text(file_path: str) -> str:
    doc = Document(file_path)
    parts = [p.text for p in doc.paragraphs if p.text]
    return "\n".join(parts).strip()

# ----------------------------
# UI
# ----------------------------
files = st.file_uploader(
    "Upload files (CSV, XLSX, PDF, DOCX)",
    type=["csv", "xlsx", "xls", "pdf", "docx"],
    accept_multiple_files=True
)

if files:
    if st.button("Process & Save"):
        conn = get_db_conn()

        for f in files:
            try:
                dtype = guess_dataset_type(f.name)
                ftype = filetype(f.name)

                # Save file to disk first -> we always get stored_path
                stored_path = save_uploaded_file(f)

                # DOCUMENTS: save text
                if dtype == "document":
                    upload_id = insert_upload(
                        conn, f.name, ftype, "document",
                        row_count=0, col_count=0,
                        stored_path=stored_path
                    )

                    text = ""
                    if ftype == "pdf":
                        text = extract_pdf_text(stored_path)
                    elif ftype == "docx":
                        text = extract_docx_text(stored_path)

                    insert_document_text(conn, upload_id, text)
                    st.success(f"✅ {f.name}: stored document text ({len(text)} chars).")

                # TABLES: residential wired now
                else:
                    df_raw = read_table_file(f)
                    df = robust_prepare(df_raw)

                    if dtype == "residential":
                        df = coerce_types_residential(df)
                        df = df[REQUIRED_RESIDENTIAL].copy()
                        df = df.dropna(subset=["address", "price"], how="all")

                        upload_id = insert_upload(
                            conn, f.name, ftype, "residential",
                            row_count=int(df.shape[0]), col_count=int(df.shape[1]),
                            stored_path=stored_path
                        )

                        df["upload_id"] = upload_id
                        df["created_at"] = datetime.utcnow()

                        rows = df.to_dict(orient="records")
                        allowed_cols = [
                            "upload_id", "ml_number", "status", "address", "city", "county", "zip",
                            "price", "sqft", "beds", "baths", "year_built", "created_at"
                        ]
                        inserted = bulk_insert_dicts(conn, "residential_listings", rows, allowed_cols)
                        st.success(f"✅ {f.name}: inserted {inserted} residential rows.")

                    else:
                        # Land/Agent wired next step, but still record upload
                        upload_id = insert_upload(
                            conn, f.name, ftype, dtype,
                            row_count=int(df.shape[0]), col_count=int(df.shape[1]),
                            stored_path=stored_path
                        )
                        st.warning(f"⚠️ {f.name}: detected '{dtype}'. Upload recorded (upload_id={upload_id}). Land/Agent row inserts are next step.")

            except Exception as e:
                conn.rollback()
                st.error(f"❌ {f.name}: {e}")

        conn.close()
else:
    st.info("Upload files to begin.")
