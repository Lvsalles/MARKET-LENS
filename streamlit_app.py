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

from db import get_db_conn, insert_upload, insert_document_text, bulk_insert_dicts

st.set_page_config(page_title="MARKET LENS — Upload Center", layout="wide")

st.title("MARKET LENS — Upload Center")
st.caption("Upload CSV/XLSX for structured data, and PDF/DOCX for document storage + text extraction.")

# Local storage (ephemeral on Streamlit Cloud). Next phase: Supabase Storage.
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)


def save_uploaded_file(uploaded_file) -> str:
    safe_name = uploaded_file.name.replace("/", "_").replace("\\", "_")
    stored_path = os.path.join(UPLOAD_DIR, f"{uuid.uuid4()}_{safe_name}")
    with open(stored_path, "wb") as out:
        out.write(uploaded_file.getbuffer())
    return stored_path


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


# ---------- Text extraction ----------
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


# ---------- Column normalization ----------
def norm(s: str) -> str:
    s = str(s).strip().lower()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9_]", "", s)
    return s


def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [norm(c) for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()].copy()
    return df


SYNONYMS = {
    # shared
    "ml_number": ["ml_number", "mls_number", "mls", "listing_id", "listingnumber", "listing_number"],
    "status": ["status", "mlsstatus", "listing_status"],
    "address": ["address", "full_address", "street_address", "unparsedaddress"],
    "city": ["city"],
    "county": ["county", "countyorparish"],
    "zip": ["zip", "zipcode", "postalcode", "postal_code"],

    # residential
    "price": ["price", "list_price", "current_price"],
    "sqft": ["sqft", "living_area", "heated_area", "heated_areanum"],
    "beds": ["beds", "bedrooms"],
    "baths": ["baths", "bathrooms", "full_baths", "fullbaths"],
    "year_built": ["year_built", "yearbuilt"],

    # land
    "acreage": ["acreage", "acres", "lot_acres"],
    "lot_sqft": ["lot_sqft", "lot_size_sqft", "lot_size_square_footage"],
    "zoning": ["zoning", "zoning_code", "land_use"],

    # agent
    "agent_name": ["agent_name", "list_agent", "agent", "agentfullname", "agent_full_name"],
    "office_name": ["office_name", "list_office_name", "office", "brokerage"],
    "agent_id": ["agent_id", "list_agent_id", "agentlicense", "license"],
}


def apply_synonyms(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    colmap = {}
    cols = set(df.columns)
    for canonical, candidates in SYNONYMS.items():
        found = next((c for c in candidates if c in cols), None)
        if found:
            colmap[found] = canonical
    if colmap:
        df = df.rename(columns=colmap)
    return df


def detect_dataset_type(df: pd.DataFrame, filename: str) -> str:
    fn = filename.lower()
    cols = set(df.columns)

    if fn.endswith(".pdf") or fn.endswith(".docx"):
        return "document"

    agent_score = 0
    if "agent_name" in cols: agent_score += 2
    if "agent_id" in cols: agent_score += 1
    if "office_name" in cols: agent_score += 1

    land_score = 0
    if "acreage" in cols: land_score += 2
    if "zoning" in cols: land_score += 1
    if "lot_sqft" in cols: land_score += 1

    res_score = 0
    if "price" in cols: res_score += 1
    if "sqft" in cols: res_score += 1
    if "beds" in cols: res_score += 1
    if "baths" in cols: res_score += 1

    scores = {"agent": agent_score, "land": land_score, "residential": res_score}
    best = max(scores, key=scores.get)

    if scores[best] < 2:
        if "agent" in fn:
            return "agent"
        if "land" in fn:
            return "land"
        return "residential"

    return best


# ---------- Safe coercions ----------
def to_num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype(str).str.replace(r"[$,]", "", regex=True),
        errors="coerce"
    )


def to_int_nullable(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    # Use pandas nullable integer to avoid overflow + allow nulls
    return s.round().astype("Int64")


def coerce_residential(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Ensure columns exist
    for c in ["ml_number", "status", "address", "city", "county", "zip"]:
        if c not in df.columns:
            df[c] = None

    for c in ["price", "sqft", "beds", "baths", "year_built"]:
        if c not in df.columns:
            df[c] = np.nan

    df["price"] = to_num(df["price"])
    df["sqft"] = to_num(df["sqft"])
    df["beds"] = to_int_nullable(df["beds"])
    df["baths"] = to_num(df["baths"])
    df["year_built"] = to_int_nullable(df["year_built"])

    return df


def coerce_land(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for c in ["ml_number", "status", "address", "city", "county", "zip", "zoning"]:
        if c not in df.columns:
            df[c] = None

    for c in ["price", "acreage", "lot_sqft"]:
        if c not in df.columns:
            df[c] = np.nan

    df["price"] = to_num(df["price"])
    df["acreage"] = to_num(df["acreage"])
    df["lot_sqft"] = to_num(df["lot_sqft"])

    return df


def coerce_agent(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in ["agent_name", "agent_id", "office_name", "city", "county"]:
        if c not in df.columns:
            df[c] = None
    return df


# ---------- UI ----------
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
    now = datetime.utcnow()

    for f in files:
        try:
            ftype = detect_filetype(f.name)

            # Always store file first so stored_path is never null
            stored_path = save_uploaded_file(f)

            # Documents
            if ftype in ("pdf", "docx"):
                upload_id = insert_upload(
                    conn,
                    filename=f.name,
                    filetype=ftype,
                    dataset_type="document",
                    row_count=0,
                    col_count=0,
                    stored_path=stored_path,
                )
                text = extract_pdf_text(stored_path) if ftype == "pdf" else extract_docx_text(stored_path)
                insert_document_text(conn, upload_id, text)
                st.success(f"✅ {f.name}: stored document + extracted text ({len(text)} chars).")
                continue

            # Structured data
            df_raw = pd.read_csv(stored_path) if ftype == "csv" else pd.read_excel(stored_path)
            df = normalize_headers(df_raw)
            df = apply_synonyms(df)

            dtype = detect_dataset_type(df, f.name)

            upload_id = insert_upload(
                conn,
                filename=f.name,
                filetype=ftype,
                dataset_type=dtype,
                row_count=int(df.shape[0]),
                col_count=int(df.shape[1]),
                stored_path=stored_path,
            )

            if dtype == "residential":
                df = coerce_residential(df)
                df["upload_id"] = upload_id
                df["created_at"] = now

                # NOTE: We do NOT include "id" to avoid identity/overflow issues.
                cols = [
                    "upload_id", "ml_number", "status", "address", "city", "county", "zip",
                    "price", "sqft", "beds", "baths", "year_built", "created_at"
                ]

                for c in cols:
                    if c not in df.columns:
                        df[c] = None

                rows = df[cols].to_dict(orient="records")
                inserted = bulk_insert_dicts(conn, "residential_listings", rows)
                st.success(f"✅ {f.name}: detected RESIDENTIAL → inserted {inserted} rows.")
                continue

            if dtype == "land":
                df = coerce_land(df)
                df["upload_id"] = upload_id
                df["created_at"] = now

                cols = [
                    "upload_id", "ml_number", "status", "address", "city", "county", "zip",
                    "price", "acreage", "lot_sqft", "zoning", "created_at"
                ]
                for c in cols:
                    if c not in df.columns:
                        df[c] = None

                rows = df[cols].to_dict(orient="records")
                inserted = bulk_insert_dicts(conn, "land_listings", rows)
                st.success(f"✅ {f.name}: detected LAND → inserted {inserted} rows.")
                continue

            if dtype == "agent":
                df = coerce_agent(df)
                df["upload_id"] = upload_id
                df["created_at"] = now

                cols = ["upload_id", "agent_name", "agent_id", "office_name", "city", "county", "created_at"]
                for c in cols:
                    if c not in df.columns:
                        df[c] = None

                rows = df[cols].to_dict(orient="records")
                inserted = bulk_insert_dicts(conn, "agent_records", rows)
                st.success(f"✅ {f.name}: detected AGENT → inserted {inserted} rows.")
                continue

            st.warning(f"⚠️ {f.name}: dataset detected as '{dtype}'. Upload recorded only (upload_id={upload_id}).")

        except Exception as e:
            conn.rollback()
            st.error(f"❌ {f.name}: {e}")

    conn.close()
