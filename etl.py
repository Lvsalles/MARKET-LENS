import hashlib
import re
import pandas as pd
from sqlalchemy import text

CANON_COLS = [
    "project_id",
    "source_file",
    "row_hash",

    # identificação / localização
    "mls_id",
    "address",
    "city",
    "state",
    "zip",
    "subdivision",

    # status e tipo
    "raw_status",
    "status_norm",      # SOLD / ACTIVE / PENDING / RENTAL / LAND / OTHER
    "property_type",

    # métricas
    "price",
    "sold_price",
    "sqft",
    "beds",
    "baths",
    "garage",
    "year_built",
    "adom",
    "sp_lp",

    # datas úteis
    "list_date",
    "pending_date",
    "sold_date",

    # raw
    "raw_json",
]


def ensure_schema(engine):
    """
    Cria tabela stg_mls se não existir.
    Usa row_hash UNIQUE para impedir duplicidade.
    """
    sql = """
    CREATE TABLE IF NOT EXISTS stg_mls (
        id BIGSERIAL PRIMARY KEY,
        project_id TEXT,
        source_file TEXT,
        row_hash TEXT UNIQUE,

        mls_id TEXT,
        address TEXT,
        city TEXT,
        state TEXT,
        zip TEXT,
        subdivision TEXT,

        raw_status TEXT,
        status_norm TEXT,
        property_type TEXT,

        price NUMERIC,
        sold_price NUMERIC,
        sqft NUMERIC,
        beds NUMERIC,
        baths NUMERIC,
        garage NUMERIC,
        year_built NUMERIC,
        adom NUMERIC,
        sp_lp NUMERIC,

        list_date DATE,
        pending_date DATE,
        sold_date DATE,

        raw_json JSONB
    );
    """
    with engine.begin() as conn:
        conn.execute(text(sql))


def _norm_name(s: str) -> str:
    s = str(s).strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_norm_name(c) for c in df.columns]
    return df


def _first_present(df: pd.DataFrame, candidates: list[str]):
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _to_num(series):
    return pd.to_numeric(series, errors="coerce")


def _to_date(series):
    return pd.to_datetime(series, errors="coerce").dt.date


def detect_and_map(df: pd.DataFrame) -> pd.DataFrame:
    """
    Mapeia colunas do MLS para colunas canônicas.
    Funciona mesmo com variações de nome.
    """
    df = normalize_columns(df)

    col_status = _first_present(df, ["status", "current_status", "mls_status", "listing_status"])
    col_type   = _first_present(df, ["property_type", "prop_type", "type"])
    col_addr   = _first_present(df, ["address", "street_address", "full_address", "address_full"])
    col_city   = _first_present(df, ["city"])
    col_state  = _first_present(df, ["state"])
    col_zip    = _first_present(df, ["zip", "zipcode", "postal_code"])
    col_subdiv = _first_present(df, ["subdivision", "subdiv", "community"])

    col_mlsid  = _first_present(df, ["mls_id", "ml_number", "listing_id", "listing_number", "id"])

    col_price  = _first_present(df, ["list_price", "price", "current_price"])
    col_sold   = _first_present(df, ["sold_price", "close_price", "sale_price", "sp"])
    col_sqft   = _first_present(df, ["sqft", "sq_ft", "living_area", "heated_area"])
    col_beds   = _first_present(df, ["beds", "bedrooms"])
    col_baths  = _first_present(df, ["baths", "bathrooms"])
    col_garage = _first_present(df, ["garage", "garage_spaces"])
    col_year   = _first_present(df, ["year_built", "yr_built"])
    col_adom   = _first_present(df, ["adom", "dom", "days_on_market"])
    col_splp   = _first_present(df, ["sp_lp", "sp_lp_ratio", "sale_to_list_ratio"])

    col_list_date    = _first_present(df, ["list_date", "listing_date"])
    col_pending_date = _first_present(df, ["pending_date", "contract_date"])
    col_sold_date    = _first_present(df, ["sold_date", "close_date", "closing_date"])

    out = pd.DataFrame()
    out["mls_id"]       = df[col_mlsid].astype(str) if col_mlsid else None
    out["address"]      = df[col_addr].astype(str) if col_addr else None
    out["city"]         = df[col_city].astype(str) if col_city else None
    out["state"]        = df[col_state].astype(str) if col_state else None
    out["zip"]          = df[col_zip].astype(str) if col_zip else None
    out["subdivision"]  = df[col_subdiv].astype(str) if col_subdiv else None

    out["raw_status"]   = df[col_status].astype(str) if col_status else ""
    out["property_type"]= df[col_type].astype(str) if col_type else ""

    out["price"]      = _to_num(df[col_price]) if col_price else None
    out["sold_price"] = _to_num(df[col_sold])  if col_sold  else None
    out["sqft"]       = _to_num(df[col_sqft])  if col_sqft  else None
    out["beds"]       = _to_num(df[col_beds])  if col_beds  else None
    out["baths"]      = _to_num(df[col_baths]) if col_baths else None
    out["garage"]     = _to_num(df[col_garage]) if col_garage else None
    out["year_built"] = _to_num(df[col_year]) if col_year else None
    out["adom"]       = _to_num(df[col_adom]) if col_adom else None
    out["sp_lp"]      = _to_num(df[col_splp]) if col_splp else None

    out["list_date"]    = _to_date(df[col_list_date]) if col_list_date else None
    out["pending_date"] = _to_date(df[col_pending_date]) if col_pending_date else None
    out["sold_date"]    = _to_date(df[col_sold_date]) if col_sold_date else None

    # status_norm: reconhece SLD/ACT/PND etc + palavras
    def norm_status(s: str, ptype: str) -> str:
        t = f"{s} {ptype}".upper()

        # tokens
        if re.search(r"\bSLD\b", t) or "SOLD" in t or "CLOSED" in t:
            return "SOLD"
        if re.search(r"\bACT\b", t) or "ACTIVE" in t:
            return "ACTIVE"
        if re.search(r"\bPND\b", t) or "PENDING" in t:
            return "PENDING"
        if "RENT" in t or re.search(r"\bRNT\b", t):
            return "RENTAL"
        if "LAND" in t or "LOT" in t or re.search(r"\bLND\b", t):
            return "LAND"
        return "OTHER"

    out["status_norm"] = [
        norm_status(a, b)
        for a, b in zip(out["raw_status"].fillna(""), out["property_type"].fillna(""))
    ]

    return out


def compute_row_hash(row: pd.Series) -> str:
    """
    Hash robusto para impedir duplicidade.
    Usa: project_id + source_file + mls_id + address + status_norm + price + sold_price
    """
    parts = [
        str(row.get("project_id", "")),
        str(row.get("source_file", "")),
        str(row.get("mls_id", "")),
        str(row.get("address", "")),
        str(row.get("status_norm", "")),
        str(row.get("price", "")),
        str(row.get("sold_price", "")),
    ]
    raw = "|".join(parts).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def insert_into_staging(engine, df_canon: pd.DataFrame, project_id: str, source_file: str) -> dict:
    """
    Insere no stg_mls com dedupe pelo row_hash (UNIQUE).
    Retorna stats de inserção.
    """
    ensure_schema(engine)

    df = df_canon.copy()
    df["project_id"] = project_id
    df["source_file"] = source_file

    # raw_json para auditoria/debug (linha inteira)
    df["raw_json"] = df.fillna("").to_dict(orient="records")

    # row_hash
    df["row_hash"] = df.apply(compute_row_hash, axis=1)

    # garante todas as colunas canônicas
    for c in CANON_COLS:
        if c not in df.columns:
            df[c] = None

    df = df[CANON_COLS]

    inserted = 0
    skipped = 0

    insert_sql = text("""
        INSERT INTO stg_mls (
            project_id, source_file, row_hash,
            mls_id, address, city, state, zip, subdivision,
            raw_status, status_norm, property_type,
            price, sold_price, sqft, beds, baths, garage, year_built, adom, sp_lp,
            list_date, pending_date, sold_date,
            raw_json
        )
        VALUES (
            :project_id, :source_file, :row_hash,
            :mls_id, :address, :city, :state, :zip, :subdivision,
            :raw_status, :status_norm, :property_type,
            :price, :sold_price, :sqft, :beds, :baths, :garage, :year_built, :adom, :sp_lp,
            :list_date, :pending_date, :sold_date,
            CAST(:raw_json AS jsonb)
        )
        ON CONFLICT (row_hash) DO NOTHING
    """)

    with engine.begin() as conn:
        for rec in df.to_dict(orient="records"):
            res = conn.execute(insert_sql, rec)
            # Em Postgres, rowcount = 1 se inseriu, 0 se conflit
            if res.rowcount == 1:
                inserted += 1
            else:
                skipped += 1

    return {"inserted": inserted, "skipped_duplicates": skipped, "total_rows_in_file": len(df_canon)}
