# streamlit_app.py
import os
import re
import uuid
from datetime import datetime

import streamlit as st
import pandas as pd
import numpy as np
from pypdf import PdfReader
from docx import Document

from db import (
    get_db_conn,
    insert_upload,
    insert_document_text,
    bulk_insert_dicts
)

# ============================================================
# Streamlit setup
# ============================================================
st.set_page_config(
    page_title="MARKET LENS — Upload Center",
    layout="wide"
)

st.title("MARKET LENS — Upload Center")
st.caption(
    "Upload CSV/XLSX for structured data, and PDF/DOCX for document storage + text extraction."
)

# ============================================================
# Local storage (ephemeral on Streamlit Cloud)
# ============================================================
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)


def save_uploaded_file(uploaded_file) -> str:
    safe_name = uploaded_file.name.replace("/", "_").replace("\\", "_")
    stored_path = os.path.join(
        UPLOAD_DIR,
        f"{uuid.uuid4()}_{safe_name}"
    )
    with open(stored_path, "wb") as out:
        out.write(uploaded_file.getbuffer())
    return stored_path


# ============================================================
# File type detection
# ============================================================
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


# ============================================================
# Text extraction
# ============================================================
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


# ============================================================
# Column normalization
# ============================================================
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


# ============================================================
# Synonyms dictionary (Layer 2 — semantic normalization)
# ============================================================
SYNONYMS = {
    # Shared
    "ml_number": ["ml_number", "mls_number", "listing_id"],
    "status": ["status", "mlsstatus"],
    "address": ["address", "full_address"],
    "city": ["city"],
    "county": ["county"],
    "zip": ["zip", "zipcode", "postalcode"],

    # Residential
    "price": ["price", "list_price"],
    "sqft": ["sqft", "living_area"],
    "beds": ["beds", "bedrooms"],
    "baths": ["baths", "bathrooms"],
    "year_built": ["year_built", "yearbuilt"],

    # Land
    "acreage": ["acreage", "acres"],
    "lot_sqft": ["lot_sqft", "lot_size_sqft"],
    "zoning": ["zoning"],

    # Agent
    "agent_id": ["agent_id", "list_agent_id", "license"],
    "agent_name": ["agent_name", "list_agent"],
    "office_id": ["office_id", "list_office_id"],
    "office_name": ["office_name", "list_office"],
}


def apply_synonyms(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    colmap = {}
    for canonical, variants in SYNONYMS.items():
        for v in variants:
            if v in df.columns:
                colmap[v] = canonical
                break
    if colmap:
        df = df.rename(columns=colmap)
    return df


# ============================================================
# Dataset detection
# ============================================================
def detect_dataset_type(df: pd.DataFrame, filename: str) -> str:
    cols = set(df.columns)
    fn = filename.lower()

    if fn.endswith(".pdf") or fn.endswith(".docx"):
        return "document"

    if "agent_name" in cols or "agent_id" in cols:
        return "agent"

    if "acreage" in cols or "lot_sqft" in cols:
        return "land"

    return "residential"


# ============================================================
# Coercion helpers
# ============================================================
def to_num(series):
    return pd.to_numeric(
        series.astype(str).str.replace(r"[$,]", "", regex=True),
        errors="coerce"
    )


def coerce_common(df: pd.DataFrame) -> pd.DataFrame:
    if "price" in df.columns:
        df["price"] = to_num(df["price"])
    if "sqft" in df.columns:
        df["sqft"] = to_num(df["sqft"])
    if "beds" in df.columns:
        df["beds"] = to_num(df["beds"])
    if "baths" in df.columns:
        df["baths"] = to_num(df["baths"])
    if "year_built" in df.columns:
        df["year_built"] = to_num(df["year_built"])
    if "acreage" in df.columns:
        df["acreage"] = to_num(df["acreage"])
    if "lot_sqft" in df.columns:
        df["lot_sqft"] = to_num(df["lot_sqft"])
    return df


# ============================================================
# UI
# ============================================================
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

    for f in files:
        try:
            ftype = detect_filetype(f.name)
            stored_path = save_uploaded_file(f)

            # --------------------------------------------------
            # Documents
            # --------------------------------------------------
            if ftype in ("pdf", "docx"):
                upload_id = insert_upload(
                    conn,
                    filename=f.name,
                    filetype=ftype,
                    dataset_type="document",
                    row_count=0,
                    col_count=0,
                    stored_path=stored_path
                )

                text = (
                    extract_pdf_text(stored_path)
                    if ftype == "pdf"
                    else extract_docx_text(stored_path)
                )

                insert_document_text(conn, upload_id, text)
                st.success(f"✅ {f.name}: document stored")
                continue

            # --------------------------------------------------
            # Structured files
            # --------------------------------------------------
            df_raw = (
                pd.read_csv(f)
                if ftype == "csv"
                else pd.read_excel(f)
            )

            df = normalize_headers(df_raw)
            df = apply_synonyms(df)
            df = coerce_common(df)

            dtype = detect_dataset_type(df, f.name)

            upload_id = insert_upload(
                conn,
                filename=f.name,
                filetype=ftype,
                dataset_type=dtype,
                row_count=len(df),
                col_count=len(df.columns),
                stored_path=stored_path
            )

            df["upload_id"] = upload_id
            df["created_at"] = datetime.utcnow()

            # --------------------------------------------------
            # Residential
            # --------------------------------------------------
            if dtype == "residential":
                allowed = [
                    "upload_id", "ml_number", "status", "address",
                    "city", "county", "zip", "price", "sqft",
                    "beds", "baths", "year_built",
                    "agent_id", "agent_name",
                    "office_id", "office_name",
                    "created_at"
                ]
                rows = df.reindex(columns=allowed).to_dict("records")
                inserted = bulk_insert_dicts(
                    conn,
                    "residential_listings",
                    rows,
                    allowed
                )
                st.success(f"✅ {f.name}: RESIDENTIAL → {inserted} rows")

            # --------------------------------------------------
            # Land
            # --------------------------------------------------
            elif dtype == "land":
                allowed = [
                    "upload_id", "ml_number", "status",
                    "city", "county", "zip",
                    "price", "acreage", "lot_sqft", "zoning",
                    "agent_id", "agent_name",
                    "office_id", "office_name",
                    "created_at"
                ]
                rows = df.reindex(columns=allowed).to_dict("records")
                inserted = bulk_insert_dicts(
                    conn,
                    "land_listings",
                    rows,
                    allowed
                )
                st.success(f"✅ {f.name}: LAND → {inserted} rows")

            # --------------------------------------------------
            # Agent
            # --------------------------------------------------
            elif dtype == "agent":
                allowed = [
                    "upload_id",
                    "agent_id", "agent_name",
                    "office_id", "office_name",
                    "city", "county",
                    "created_at"
                ]
                rows = df.reindex(columns=allowed).to_dict("records")
                inserted = bulk_insert_dicts(
                    conn,
                    "agent_records",
                    rows,
                    allowed
                )
                st.success(f"✅ {f.name}: AGENT → {inserted} rows")

            else:
                st.warning(f"⚠️ {f.name}: dataset detected as {dtype}, not inserted")

        except Exception as e:
            conn.rollback()
            st.error(f"❌ {f.name}: {e}")

    conn.close()
    st.success("Done.")
