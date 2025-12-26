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


# ----------------------------
# Local storage (ephemeral on Streamlit Cloud)
# ----------------------------
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
    "ml_number": ["ml_number", "mls_number", "mls", "listing_id", "listingnumber", "mlnumber"],
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
    "acreage": ["acreage", "acres", "total_acreage", "lot_acres"],
    "lot_sqft": ["lot_sqft", "lot_size_sqft", "lot_size_square_footage", "lot_size_square_footage_num"],
    "zoning": ["zoning", "zoning_code", "land_use"],

    # agent
    "agent_name": ["agent_name", "list_agent", "agent", "agentfullname", "agent_full_name"],
    "office_name": ["office_name", "list_office_name", "office", "brokerage"],
    "agent_id": ["agent_id", "list_agent_id", "agentlicense", "license", "list_agent_id"],
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
    """
    IMPORTANT:
    - Filename-based override for AGENT and LAND (to avoid misclassification).
    - Otherwise, use column scoring.
    """
    fn = filename.lower()

    # Strong filename overrides
    if "agent" in fn:
        return "agent"
    if "land" in fn:
        return "land"

    cols = set(df.columns)

    # agent scoring
    agent_score = 0
    if "agent_name" in cols:
        agent_score += 3
    if "agent_id" in cols:
        agent_score += 2
    if "office_name" in cols:
        agent_score += 1

    # land scoring
    land_score = 0
    if "acreage" in cols:
        land_score += 3
    if "zoning" in cols:
        land_score += 1
    if "lot_sqft" in cols:
        land_score += 1

    # residential scoring
    res_score = 0
    if "price" in cols:
        res_score += 1
    if "sqft" in cols:
        res_score += 1
    if "beds" in cols:
        res_score += 1
    if "baths" in cols:
        res_score += 1

    scores = {"agent": agent_score, "land": land_score, "residential": res_score}
    best = max(scores, key=scores.get)

    # default fallback
    return best


# ----------------------------
# Coercions
# ----------------------------
def to_num(series):
    return pd.to_numeric(series.astype(str).str.replace(r"[$,]", "", regex=True), errors="coerce")


def ensure_cols(df: pd.DataFrame, cols: list[str], fill_value=None) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        if c not in df.columns:
            df[c] = fill_value
    return df


def coerce_residential(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = ensure_cols(df, ["ml_number", "status", "address", "city", "county", "zip"], None)
    df = ensure_cols(df, ["price", "sqft", "beds", "baths", "year_built"], np.nan)

    df["price"] = to_num(df["price"])
    df["sqft"] = to_num(df["sqft"])
    df["beds"] = to_num(df["beds"])
    df["baths"] = to_num(df["baths"])
    df["year_built"] = to_num(df["year_built"])

    return df


def coerce_land(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = ensure_cols(df, ["ml_number", "status", "address", "city", "county", "zip", "zoning"], None)
    df = ensure_cols(df, ["price", "acreage", "lot_sqft"], np.nan)

    df["price"] = to_num(df["price"])
    df["acreage"] = to_num(df["acreage"])
    df["lot_sqft"] = to_num(df["lot_sqft"])

    return df


def coerce_agent(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = ensure_cols(df, ["agent_name", "agent_id", "office_name", "city", "county"], None)
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
    report_id = uuid.uuid4()
    st.write(f"**Report ID:** `{report_id}`")

    conn = get_db_conn()

    for f in files:
        try:
            ftype = detect_filetype(f.name)
            stored_path = save_uploaded_file(f)

            # Documents
            if ftype in ("pdf", "docx"):
                upload_id = insert_upload(
                    conn,
                    report_id=report_id,
                    filename=f.name,
                    filetype=ftype,
                    dataset_type="document",
                    row_count=0,
                    col_count=0,
                    stored_path=stored_path,
                )

                text = extract_pdf_text(stored_path) if ftype == "pdf" else extract_docx_text(stored_path)
                insert_document_text(conn, upload_id=upload_id, text=text)
                st.success(f"✅ {f.name}: stored document + extracted text ({len(text)} chars).")
                continue

            # Structured files
            df_raw = pd.read_csv(f) if ftype == "csv" else pd.read_excel(f)
            df = normalize_headers(df_raw)
            df = apply_synonyms(df)

            dtype = detect_dataset_type(df, f.name)

            upload_id = insert_upload(
                conn,
                report_id=report_id,
                filename=f.name,
                filetype=ftype,
                dataset_type=dtype,
                row_count=int(df.shape[0]),
                col_count=int(df.shape[1]),
                stored_path=stored_path,
            )

            now = datetime.utcnow()

            if dtype == "residential":
                df = coerce_residential(df)
                df["upload_id"] = upload_id
                df["report_id"] = str(report_id)
                df["created_at"] = now

                allowed_cols = [
                    "report_id",
                    "upload_id",
                    "ml_number",
                    "status",
                    "address",
                    "city",
                    "county",
                    "zip",
                    "price",
                    "sqft",
                    "beds",
                    "baths",
                    "year_built",
                    "created_at",
                ]

                rows = df[allowed_cols].to_dict(orient="records")
                inserted = bulk_insert_dicts(conn, table="residential_listings", rows=rows, allowed_cols=allowed_cols)
                st.success(f"✅ {f.name}: detected RESIDENTIAL → inserted {inserted} rows.")
                continue

            if dtype == "land":
                df = coerce_land(df)
                df["upload_id"] = upload_id
                df["report_id"] = str(report_id)
                df["created_at"] = now

                allowed_cols = [
                    "report_id",
                    "upload_id",
                    "ml_number",
                    "status",
                    "address",
                    "city",
                    "county",
                    "zip",
                    "price",
                    "acreage",
                    "lot_sqft",
                    "zoning",
                    "created_at",
                ]

                df = ensure_cols(df, allowed_cols, None)
                rows = df[allowed_cols].to_dict(orient="records")
                inserted = bulk_insert_dicts(conn, table="land_listings", rows=rows, allowed_cols=allowed_cols)
                st.success(f"✅ {f.name}: detected LAND → inserted {inserted} rows.")
                continue

            if dtype == "agent":
                df = coerce_agent(df)
                df["upload_id"] = upload_id
                df["report_id"] = str(report_id)
                df["created_at"] = now

                allowed_cols = [
                    "report_id",
                    "upload_id",
                    "agent_name",
                    "agent_id",
                    "office_name",
                    "city",
                    "county",
                    "created_at",
                ]

                df = ensure_cols(df, allowed_cols, None)
                rows = df[allowed_cols].to_dict(orient="records")
                inserted = bulk_insert_dicts(conn, table="agent_records", rows=rows, allowed_cols=allowed_cols)
                st.success(f"✅ {f.name}: detected AGENT → inserted {inserted} rows.")
                continue

            st.warning(f"⚠️ {f.name}: dataset detected as '{dtype}', upload recorded only (upload_id={upload_id}).")

        except Exception as e:
            conn.rollback()
            st.error(f"❌ {f.name}: {e}")

    conn.close()
    st.success("Done.")
