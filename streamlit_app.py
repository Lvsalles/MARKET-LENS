# streamlit_app.py
import os
import re
import uuid
import hashlib
from datetime import datetime, timezone

import streamlit as st
import pandas as pd
import numpy as np
from pypdf import PdfReader
from docx import Document

from db import (
    get_db_conn,
    upload_exists_by_hash,
    get_upload_id_by_hash,
    insert_upload,
    insert_document_text,
    bulk_insert_dicts,
    log_run_start,
    log_run_end,
    log_event,
)

st.set_page_config(page_title="MARKET LENS — Upload Center", layout="wide")

st.title("MARKET LENS — Upload Center")
st.caption("Upload CSV/XLSX for structured data, and PDF/DOCX for document storage + text extraction.")

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)


# ----------------------------
# Utilities
# ----------------------------
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


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def save_uploaded_file_bytes(filename: str, data: bytes) -> str:
    safe_name = filename.replace("/", "_").replace("\\", "_")
    stored_path = os.path.join(UPLOAD_DIR, f"{uuid.uuid4()}_{safe_name}")
    with open(stored_path, "wb") as out:
        out.write(data)
    return stored_path


# ----------------------------
# Text extraction
# ----------------------------
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
# Column normalization
# ----------------------------
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
    "ml_number": ["ml_number", "mls_number", "mls", "listing_id", "listingnumber", "ml_number_1"],
    "status": ["status", "mlsstatus", "listing_status"],
    "address": ["address", "full_address", "street_address", "unparsedaddress"],
    "city": ["city"],
    "county": ["county", "countyorparish"],
    "zip": ["zip", "zipcode", "postalcode", "postal_code"],

    # residential
    "price": ["price", "list_price", "current_price"],
    "sqft": ["sqft", "living_area", "heated_area", "heated_areanum"],
    "beds": ["beds", "bedrooms"],
    "baths": ["baths", "bathrooms", "full_baths"],
    "year_built": ["year_built", "yearbuilt"],

    # land
    "acreage": ["acreage", "acres", "lot_acres", "total_acreage"],
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

    agent_score = 0
    if "agent_name" in cols:
        agent_score += 2
    if "agent_id" in cols:
        agent_score += 1
    if "office_name" in cols:
        agent_score += 1

    land_score = 0
    if "acreage" in cols:
        land_score += 2
    if "zoning" in cols:
        land_score += 1
    if "lot_sqft" in cols:
        land_score += 1

    res_score = 0
    for k in ["price", "sqft", "beds", "baths"]:
        if k in cols:
            res_score += 1

    scores = {"agent": agent_score, "land": land_score, "residential": res_score}
    best = max(scores, key=scores.get)

    if scores[best] < 2:
        if "agent" in fn:
            return "agent"
        if "land" in fn:
            return "land"
        return "residential"

    return best


# ----------------------------
# Coercions
# ----------------------------
def to_num(series):
    return pd.to_numeric(series.astype(str).str.replace(r"[$,]", "", regex=True), errors="coerce")


def coerce_residential(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in ["ml_number", "status", "address", "city", "county", "zip", "year_built"]:
        if c not in df.columns:
            df[c] = None
    for c in ["price", "sqft", "beds", "baths"]:
        if c not in df.columns:
            df[c] = np.nan

    df["price"] = to_num(df["price"])
    df["sqft"] = to_num(df["sqft"])
    df["beds"] = to_num(df["beds"])
    df["baths"] = to_num(df["baths"])
    df["year_built"] = to_num(df["year_built"])
    return df


def coerce_land(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in ["ml_number", "status", "address", "city", "county", "zip", "zoning"]:
        if c not in df.columns:
            df[c] = None
    for c in ["price", "acreage", "lot_sqft"]:
        if c not in df.columns:
            df[c] = np.nan

    df["price"] = to_num(df["price"]) if "price" in df.columns else np.nan
    df["acreage"] = to_num(df["acreage"])
    df["lot_sqft"] = to_num(df["lot_sqft"])
    return df


def coerce_agent(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in ["agent_name", "agent_id", "office_name", "city", "county"]:
        if c not in df.columns:
            df[c] = None
    return df


# ----------------------------
# UI
# ----------------------------
files = st.file_uploader(
    "Upload files (CSV, XLSX, PDF, DOCX)",
    type=["csv", "xlsx", "xls", "pdf", "docx"],
    accept_multiple_files=True,
)

if not files:
    st.info("Upload files to begin.")
    st.stop()

if st.button("Process & Save"):
    conn = get_db_conn()
    run_id = log_run_start(conn, notes="Upload Center run")

    try:
        for f in files:
            try:
                # Read bytes once (so we can hash + save + parse safely)
                raw_bytes = f.getvalue()
                file_hash = sha256_bytes(raw_bytes)
                ftype = detect_filetype(f.name)

                # 1) DEDUPE: if file_hash exists, skip ingestion (prevents 3015 * 8 issue)
                if upload_exists_by_hash(conn, file_hash):
                    existing_id = get_upload_id_by_hash(conn, file_hash)
                    st.warning(f"⚠️ {f.name}: already ingested (upload_id={existing_id}). Skipping.")
                    log_event(conn, run_id, existing_id, "warning", f"Skipped duplicate by file_hash for {f.name}")
                    continue

                # 2) Always save to disk first (stored_path never NULL)
                stored_path = save_uploaded_file_bytes(f.name, raw_bytes)

                # 3) Documents
                if ftype in ("pdf", "docx"):
                    upload_id = insert_upload(
                        conn=conn,
                        filename=f.name,
                        filetype=ftype,
                        dataset_type="document",
                        row_count=0,
                        col_count=0,
                        stored_path=stored_path,
                        file_hash=file_hash,
                    )
                    text = extract_pdf_text(stored_path) if ftype == "pdf" else extract_docx_text(stored_path)
                    insert_document_text(conn, upload_id, text)
                    st.success(f"✅ {f.name}: stored document + extracted text ({len(text)} chars).")
                    log_event(conn, run_id, upload_id, "info", f"Document stored: {f.name}")
                    continue

                # 4) Structured files
                if ftype == "csv":
                    df_raw = pd.read_csv(f)
                else:
                    df_raw = pd.read_excel(f)

                df = normalize_headers(df_raw)
                df = apply_synonyms(df)
                dtype = detect_dataset_type(df, f.name)

                # Record upload
                upload_id = insert_upload(
                    conn=conn,
                    filename=f.name,
                    filetype=ftype,
                    dataset_type=dtype,
                    row_count=int(df.shape[0]),
                    col_count=int(df.shape[1]),
                    stored_path=stored_path,
                    file_hash=file_hash,
                )

                now = datetime.now(timezone.utc)

                if dtype == "residential":
                    df2 = coerce_residential(df)
                    df2["upload_id"] = upload_id
                    df2["created_at"] = now

                    allowed_cols = [
                        "upload_id", "ml_number", "status", "address", "city", "county", "zip",
                        "price", "sqft", "beds", "baths", "year_built", "created_at"
                    ]
                    for c in allowed_cols:
                        if c not in df2.columns:
                            df2[c] = None

                    rows = df2[allowed_cols].to_dict(orient="records")
                    inserted = bulk_insert_dicts(conn, "residential_listings", rows, allowed_cols)
                    st.success(f"✅ {f.name}: detected RESIDENTIAL → inserted {inserted} rows.")
                    log_event(conn, run_id, upload_id, "info", f"Residential inserted: {inserted} rows")

                elif dtype == "land":
                    df2 = coerce_land(df)
                    df2["upload_id"] = upload_id
                    df2["created_at"] = now

                    allowed_cols = [
                        "upload_id", "ml_number", "status", "address", "city", "county", "zip",
                        "price", "acreage", "lot_sqft", "zoning", "created_at"
                    ]
                    for c in allowed_cols:
                        if c not in df2.columns:
                            df2[c] = None

                    rows = df2[allowed_cols].to_dict(orient="records")
                    inserted = bulk_insert_dicts(conn, "land_listings", rows, allowed_cols)
                    st.success(f"✅ {f.name}: detected LAND → inserted {inserted} rows.")
                    log_event(conn, run_id, upload_id, "info", f"Land inserted: {inserted} rows")

                elif dtype == "agent":
                    df2 = coerce_agent(df)
                    df2["upload_id"] = upload_id
                    df2["created_at"] = now

                    allowed_cols = ["upload_id", "agent_name", "agent_id", "office_name", "city", "county", "created_at"]
                    for c in allowed_cols:
                        if c not in df2.columns:
                            df2[c] = None

                    rows = df2[allowed_cols].to_dict(orient="records")
                    inserted = bulk_insert_dicts(conn, "agent_records", rows, allowed_cols)
                    st.success(f"✅ {f.name}: detected AGENT → inserted {inserted} rows.")
                    log_event(conn, run_id, upload_id, "info", f"Agent inserted: {inserted} rows")

                else:
                    st.warning(f"⚠️ {f.name}: dataset detected as '{dtype}', upload recorded only.")
                    log_event(conn, run_id, upload_id, "warning", f"Unknown dataset type: {dtype}")

            except Exception as e:
                conn.rollback()
                st.error(f"❌ {f.name}: {e}")
                # upload_id might not exist if it failed early; log without upload_id
                try:
                    log_event(conn, run_id, None, "error", f"Failed file: {f.name} → {e}")
                except Exception:
                    pass

        log_run_end(conn, run_id, status="success")

    except Exception as e:
        conn.rollback()
        log_run_end(conn, run_id, status="failed")
        st.error(f"Run failed: {e}")

    finally:
        conn.close()
