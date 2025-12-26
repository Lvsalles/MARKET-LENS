# streamlit_app.py
import os
import uuid
import re
from datetime import datetime

import streamlit as st
import pandas as pd
import numpy as np

from db import (
    get_db_conn,
    insert_upload,
    insert_document_text,
    bulk_insert_dicts,
)

from pypdf import PdfReader
from docx import Document

# ===============================
# PAGE CONFIG
# ===============================
st.set_page_config(
    page_title="MARKET LENS — Upload Center",
    layout="wide"
)

st.title("MARKET LENS — Upload Center")
st.caption(
    "Upload CSV/XLSX for structured data, and PDF/DOCX for document storage + text extraction."
)

# ===============================
# STORAGE (LOCAL)
# ===============================
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

def save_uploaded_file(uploaded_file) -> str:
    safe_name = uploaded_file.name.replace("/", "_").replace("\\", "_")
    stored_path = os.path.join(
        UPLOAD_DIR, f"{uuid.uuid4()}_{safe_name}"
    )
    with open(stored_path, "wb") as out:
        out.write(uploaded_file.getbuffer())
    return stored_path

# ===============================
# FILE TYPE
# ===============================
def detect_filetype(filename: str) -> str:
    fn = filename.lower().strip()
    if fn.endswith(".csv"):
        return "csv"
    if fn.endswith(".xlsx") or fn.endswith(".xls"):
        return "xlsx"
    if fn.endswith(".pdf"):
        return "pdf"
    if fn.endswith(".docx"):
        return "docx"
    return "other"

# ===============================
# TEXT EXTRACTION
# ===============================
def extract_pdf_text(path: str) -> str:
    reader = PdfReader(path)
    return "\n".join(
        page.extract_text() or "" for page in reader.pages
    ).strip()

def extract_docx_text(path: str) -> str:
    doc = Document(path)
    return "\n".join(p.text for p in doc.paragraphs if p.text).strip()

# ===============================
# HEADER NORMALIZATION
# ===============================
def norm(col: str) -> str:
    col = str(col).strip().lower()
    col = re.sub(r"\s+", "_", col)
    col = re.sub(r"[^a-z0-9_]", "", col)
    return col

def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [norm(c) for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]
    return df

# ===============================
# 🔥 CAMADA 2 — BIBLIOTECA SEMÂNTICA
# ===============================
SEMANTIC_DICTIONARY = {
    # Identificadores
    "ml_number": ["ml_number", "mls_number", "listing_id"],
    "status": ["status", "mlsstatus"],

    # Localização
    "address": ["address", "street_address", "full_address"],
    "city": ["city"],
    "county": ["county", "countyorparish"],
    "zip": ["zip", "zipcode", "postal_code"],

    # Valores
    "price": ["price", "list_price", "close_price"],
    "sqft": ["sqft", "living_area", "heated_area"],
    "beds": ["beds", "bedrooms"],
    "baths": ["baths", "bathrooms"],
    "year_built": ["year_built", "yearbuilt"],

    # Land
    "acreage": ["acreage", "acres", "lot_acres"],
    "lot_sqft": ["lot_sqft", "lot_size_sqft"],
    "zoning": ["zoning", "land_use"],

    # Agent / Office
    "agent_key": ["agent_id", "list_agent_id", "license"],
    "agent_name": ["agent", "agent_name", "list_agent"],
    "office_key": ["office_id", "list_office_id"],
    "office_name": ["office", "list_office", "brokerage"],
}

def apply_semantic_dictionary(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    cols = {c.lower(): c for c in df.columns}
    rename_map = {}

    for canonical, aliases in SEMANTIC_DICTIONARY.items():
        for a in aliases:
            if a in cols:
                rename_map[cols[a]] = canonical
                break

    return df.rename(columns=rename_map)

# ===============================
# DATASET DETECTION
# ===============================
def detect_dataset_type(df: pd.DataFrame, filename: str) -> str:
    cols = set(df.columns)
    name = filename.lower()

    if "agent" in name or "agent_name" in cols:
        return "agent"
    if "acreage" in cols or "land" in name:
        return "land"
    return "residential"

# ===============================
# COERCIONS
# ===============================
def to_num(series):
    return pd.to_numeric(
        series.astype(str).str.replace(r"[$,]", "", regex=True),
        errors="coerce"
    )

def coerce_residential(df):
    df = df.copy()
    for c in ["price", "sqft", "beds", "baths", "year_built"]:
        if c in df.columns:
            df[c] = to_num(df[c])
    return df

def coerce_land(df):
    df = df.copy()
    for c in ["price", "acreage", "lot_sqft"]:
        if c in df.columns:
            df[c] = to_num(df[c])
    return df

# ===============================
# UI
# ===============================
files = st.file_uploader(
    "Upload files (CSV, XLSX, PDF, DOCX)",
    type=["csv", "xlsx", "xls", "pdf", "docx"],
    accept_multiple_files=True
)

if not files:
    st.info("Upload files to begin.")
    st.stop()

if st.button("Process & Save"):
    conn = get_db_conn()
    report_id = str(uuid.uuid4())
    st.write(f"**Report ID:** `{report_id}`")

    for f in files:
        try:
            ftype = detect_filetype(f.name)
            stored_path = save_uploaded_file(f)

            # ===============================
            # DOCUMENTS
            # ===============================
            if ftype in ("pdf", "docx"):
                upload_id = insert_upload(
                    conn,
                    filename=f.name,
                    filetype=ftype,
                    dataset_type="document",
                    row_count=0,
                    col_count=0,
                    stored_path=stored_path,
                    report_id=report_id
                )

                text = (
                    extract_pdf_text(stored_path)
                    if ftype == "pdf"
                    else extract_docx_text(stored_path)
                )

                insert_document_text(conn, upload_id, text)
                st.success(f"✅ {f.name}: document stored")
                continue

            # ===============================
            # STRUCTURED FILES
            # ===============================
            df_raw = (
                pd.read_csv(f)
                if ftype == "csv"
                else pd.read_excel(f)
            )

            df = normalize_headers(df_raw)
            df = apply_semantic_dictionary(df)

            dtype = detect_dataset_type(df, f.name)

            upload_id = insert_upload(
                conn,
                filename=f.name,
                filetype=ftype,
                dataset_type=dtype,
                row_count=len(df),
                col_count=len(df.columns),
                stored_path=stored_path,
                report_id=report_id
            )

            df["upload_id"] = upload_id
            df["created_at"] = datetime.utcnow()

            if dtype == "residential":
                df = coerce_residential(df)
                inserted = bulk_insert_dicts(
                    conn,
                    "residential_listings",
                    df.to_dict(orient="records"),
                    df.columns.tolist()
                )
                st.success(
                    f"✅ {f.name}: detected RESIDENTIAL → inserted {inserted} rows."
                )

            elif dtype == "land":
                df = coerce_land(df)
                inserted = bulk_insert_dicts(
                    conn,
                    "land_listings",
                    df.to_dict(orient="records"),
                    df.columns.tolist()
                )
                st.success(
                    f"✅ {f.name}: detected LAND → inserted {inserted} rows."
                )

            elif dtype == "agent":
                inserted = bulk_insert_dicts(
                    conn,
                    "agent_records",
                    df.to_dict(orient="records"),
                    df.columns.tolist()
                )
                st.success(
                    f"✅ {f.name}: detected AGENT → inserted {inserted} rows."
                )

        except Exception as e:
            conn.rollback()
            st.error(f"❌ {f.name}: {e}")

    conn.close()
    st.success("Done.")
