import streamlit as st

st.write("Secrets keys loaded:", list(st.secrets.keys()))

if "DATABASE_URL" in st.secrets:
    url = st.secrets["DATABASE_URL"]
    # mask password safely
    masked = url
    if "://" in masked and "@" in masked:
        left, right = masked.split("://", 1)
        creds, host = right.split("@", 1)
        if ":" in creds:
            user, _pw = creds.split(":", 1)
            masked = f"{left}://{user}:*****@{host}"
    st.write("DATABASE_URL detected:", masked)
else:
    st.error("DATABASE_URL is missing from Streamlit secrets!")
    st.stop()

    get_db_conn,
    insert_upload,
    insert_document_text,
    bulk_insert_dicts,
    fetch_table_columns,
)

st.set_page_config(page_title="MARKET LENS — Upload Center", layout="wide")

st.title("MARKET LENS — Upload Center")
st.caption("Upload CSV/XLSX for structured data, and PDF/DOCX for document storage + text extraction.")


# ----------------------------
# Local storage (ephemeral on Streamlit Cloud)
# In the future: replace with Supabase Storage.
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
# Column normalization + synonyms
# ----------------------------
def norm_col(s: str) -> str:
    s = str(s).strip().lower()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9_]", "", s)
    return s


def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [norm_col(c) for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()].copy()
    return df


SYNONYMS = {
    # shared
    "ml_number": ["ml_number", "mls_number", "mls", "listing_id", "listingnumber"],
    "status": ["status", "mlsstatus", "listing_status"],
    "address": ["address", "full_address", "street_address", "unparsedaddress"],
    "city": ["city"],
    "county": ["county", "countyorparish"],
    "zip": ["zip", "zipcode", "postalcode", "postal_code"],

    # residential
    "price": ["price", "list_price", "current_price", "listprice"],
    "sqft": ["sqft", "living_area", "heated_area", "heated_areanum", "livingarea"],
    "beds": ["beds", "bedrooms", "bedroomstotal"],
    "baths": ["baths", "bathrooms", "bathstotal"],  # will be mapped into full_baths if needed
    "full_baths": ["full_baths", "fullbaths", "bathroomsfull"],
    "half_baths": ["half_baths", "halfbaths", "bathroomshalf"],
    "year_built": ["year_built", "yearbuilt"],

    # land
    "acreage": ["acreage", "acres", "lot_acres", "lotsizeacres"],
    "lot_sqft": ["lot_sqft", "lot_size_sqft", "lotsizesquarefeet", "lot_size_square_footage"],
    "zoning": ["zoning", "zoning_code", "land_use"],

    # agent
    "agent_name": ["agent_name", "list_agent", "agent", "agentfullname", "agent_full_name"],
    "office_name": ["office_name", "list_office_name", "office", "brokerage"],
    "agent_id": ["agent_id", "list_agent_id", "agentlicense", "license"],
}


def apply_synonyms(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    cols = set(df.columns)
    rename_map = {}

    for canonical, candidates in SYNONYMS.items():
        found = next((c for c in candidates if c in cols), None)
        if found and found != canonical:
            rename_map[found] = canonical

    if rename_map:
        df = df.rename(columns=rename_map)

    return df


def detect_dataset_type(df: pd.DataFrame, filename: str) -> str:
    """
    Detects dataset type based on columns and filename hint.
    """
    fn = filename.lower()
    cols = set(df.columns)

    # Agent (strong)
    agent_score = 0
    if "agent_name" in cols: agent_score += 4
    if "agent_id" in cols: agent_score += 3
    if "office_name" in cols: agent_score += 2

    # Land (strong)
    land_score = 0
    if "acreage" in cols: land_score += 4
    if "zoning" in cols: land_score += 2
    if "lot_sqft" in cols: land_score += 1

    # Residential (strong)
    res_score = 0
    if "price" in cols: res_score += 1
    if "sqft" in cols: res_score += 1
    if "beds" in cols: res_score += 1
    if ("baths" in cols) or ("full_baths" in cols): res_score += 1

    scores = {"agent": agent_score, "land": land_score, "residential": res_score}
    best = max(scores, key=scores.get)

    # Filename hints if weak
    if scores[best] < 3:
        if "agent" in fn:
            return "agent"
        if "land" in fn:
            return "land"
        return "residential"

    return best


# ----------------------------
# Coercions / cleaning
# ----------------------------
def to_num(series: pd.Series) -> pd.Series:
    s = series.astype(str)
    s = s.str.replace(r"[$,]", "", regex=True)
    s = s.str.replace(r"\s+", "", regex=True)
    return pd.to_numeric(s, errors="coerce")


def coerce_residential(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Ensure base text fields
    for c in ["ml_number", "status", "address", "city", "county", "zip"]:
        if c not in df.columns:
            df[c] = None

    # Ensure numeric fields exist
    for c in ["price", "sqft", "beds", "year_built"]:
        if c not in df.columns:
            df[c] = np.nan

    # Handle baths variants:
    # If the DB uses full_baths/half_baths, we should fill those.
    if "full_baths" not in df.columns:
        df["full_baths"] = df["baths"] if "baths" in df.columns else np.nan
    if "half_baths" not in df.columns:
        df["half_baths"] = np.nan

    # Coerce numeric
    df["price"] = to_num(df["price"])
    df["sqft"] = to_num(df["sqft"])
    df["full_baths"] = to_num(df["full_baths"])
    df["half_baths"] = to_num(df["half_baths"])
    df["year_built"] = to_num(df["year_built"])

    # Beds: must be safe int (prevents "integer out of range")
    beds = to_num(df["beds"])
    beds = beds.where((beds >= 0) & (beds <= 20), np.nan)
    df["beds"] = beds.round().astype("Int64")  # nullable integer

    return df


def coerce_land(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for c in ["ml_number", "status", "address", "city", "county", "zoning", "zip"]:
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


def filter_to_existing_columns(conn, table_name: str, desired_cols: list[str]) -> list[str]:
    existing = fetch_table_columns(conn, table_name)
    return [c for c in desired_cols if c in existing]


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

    # Cache table columns once per run (faster + consistent)
    res_cols = fetch_table_columns(conn, "residential_listings")
    land_cols = fetch_table_columns(conn, "land_listings")
    agent_cols = fetch_table_columns(conn, "agent_records")
    upload_cols = fetch_table_columns(conn, "uploads")
    doc_cols = fetch_table_columns(conn, "document_text")

    for f in files:
        try:
            ftype = detect_filetype(f.name)

            # Always store file first (so stored_path is never null)
            stored_path = save_uploaded_file(f)

            # DOCUMENTS
            if ftype in ("pdf", "docx"):
                upload_id = insert_upload(
                    conn=conn,
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

            # STRUCTURED (CSV/XLSX)
            df_raw = pd.read_csv(stored_path) if ftype == "csv" else pd.read_excel(stored_path)
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
            )

            now = datetime.now(timezone.utc)

            # RESIDENTIAL
            if dtype == "residential":
                df = coerce_residential(df)
                df["upload_id"] = upload_id
                df["created_at"] = now

                desired = [
                    "upload_id", "ml_number", "status", "address", "city", "county", "zip",
                    "price", "sqft", "beds",
                    "full_baths", "half_baths",
                    "year_built",
                    "created_at",
                ]

                allowed = [c for c in desired if c in res_cols]
                for c in allowed:
                    if c not in df.columns:
                        df[c] = None

                rows = df[allowed].to_dict(orient="records")
                inserted = bulk_insert_dicts(conn, "residential_listings", rows, allowed)
                st.success(f"✅ {f.name}: detected RESIDENTIAL → inserted {inserted} rows.")
                continue

            # LAND
            if dtype == "land":
                df = coerce_land(df)
                df["upload_id"] = upload_id
                df["created_at"] = now

                desired = [
                    "upload_id", "ml_number", "status", "address", "city", "county", "zip",
                    "price", "acreage", "lot_sqft", "zoning",
                    "created_at",
                ]

                allowed = [c for c in desired if c in land_cols]
                for c in allowed:
                    if c not in df.columns:
                        df[c] = None

                rows = df[allowed].to_dict(orient="records")
                inserted = bulk_insert_dicts(conn, "land_listings", rows, allowed)
                st.success(f"✅ {f.name}: detected LAND → inserted {inserted} rows.")
                continue

            # AGENT
            if dtype == "agent":
                df = coerce_agent(df)
                df["upload_id"] = upload_id
                df["created_at"] = now

                desired = [
                    "upload_id",
                    "agent_name", "agent_id", "office_name",
                    "city", "county",
                    "created_at",
                ]

                allowed = [c for c in desired if c in agent_cols]
                for c in allowed:
                    if c not in df.columns:
                        df[c] = None

                rows = df[allowed].to_dict(orient="records")
                inserted = bulk_insert_dicts(conn, "agent_records", rows, allowed)
                st.success(f"✅ {f.name}: detected AGENT → inserted {inserted} rows.")
                continue

            # FALLBACK
            st.warning(f"⚠️ {f.name}: dataset detected as '{dtype}'. Upload recorded only (upload_id={upload_id}).")

        except Exception as e:
            conn.rollback()
            st.error(f"❌ {f.name}: {e}")

    conn.close()
