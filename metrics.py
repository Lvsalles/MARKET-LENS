import re
import pandas as pd
from sqlalchemy import text


# ============================================================
# DB INTROSPECTION
# ============================================================

def table_has_column(engine, table_name: str, column_name: str) -> bool:
    q = text("""
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = :table
          AND column_name = :col
        LIMIT 1
    """)
    with engine.begin() as conn:
        return conn.execute(q, {"table": table_name, "col": column_name}).fetchone() is not None


def read_stg(engine, project_id: str):
    """
    Lê stg_mls.
    - Se existir project_id, filtra.
    - Se não existir, lê tudo.
    """
    has_project = table_has_column(engine, "stg_mls", "project_id")

    if has_project:
        sql = text("SELECT * FROM stg_mls WHERE project_id = :project_id")
        params = {"project_id": project_id}
    else:
        sql = text("SELECT * FROM stg_mls")
        params = {}

    with engine.begin() as conn:
        return pd.read_sql(sql, conn, params=params)


# ============================================================
# COLUMN DETECTION (STATUS / TYPE / PRICE / SQFT / ADOM)
# ============================================================

def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(s).strip().lower()).strip("_")


def detect_columns(df: pd.DataFrame) -> dict:
    """
    Detecta colunas prováveis no DataFrame, sem depender de nomes fixos.
    Retorna um dict com chaves:
      status_col, type_col, price_col, sqft_col, adom_col, beds_col, baths_col
    """
    cols = list(df.columns)
    ncols = {_norm(c): c for c in cols}

    # candidatos por padrões
    status_candidates = []
    type_candidates = []
    price_candidates = []
    sqft_candidates = []
    adom_candidates = []
    beds_candidates = []
    baths_candidates = []

    for c in cols:
        nc = _norm(c)

        if "status" in nc or nc in {"st", "sts"} or "mls_status" in nc:
            status_candidates.append(c)

        if "property_type" in nc or ("type" in nc and "property" in nc) or nc in {"propertytype", "proptype"}:
            type_candidates.append(c)

        if ("sold" in nc and "price" in nc) or nc in {"sold_price", "sp"}:
            price_candidates.append(c)
        if ("list" in nc and "price" in nc) or ("current" in nc and "price" in nc) or nc in {"list_price", "current_price", "price"}:
            price_candidates.append(c)

        if "sqft" in nc or "sq_ft" in nc or "heated_area" in nc or "living_area" in nc or nc in {"sqft", "sq_ft", "area"}:
            sqft_candidates.append(c)

        if "adom" in nc or ("days" in nc and "market" in nc) or nc in {"dom"}:
            adom_candidates.append(c)

        if nc in {"beds", "bedrooms", "br"} or "beds" in nc or "bed" in nc:
            beds_candidates.append(c)

        if nc in {"baths", "bathrooms"} or "baths" in nc or "bath" in nc:
            baths_candidates.append(c)

    # escolher o melhor candidato por “prioridade”
    def pick(cands, preferred_norms):
        # tenta bater por nomes preferidos (normalizados)
        for pn in preferred_norms:
            if pn in ncols:
                return ncols[pn]
        # senão pega o primeiro candidato
        return cands[0] if cands else None

    status_col = pick(status_candidates, ["status", "mls_status", "current_status", "listing_status"])
    type_col   = pick(type_candidates,   ["property_type", "propertytype", "prop_type"])
    price_col  = pick(price_candidates,  ["sold_price", "sp", "current_price", "list_price", "price"])
    sqft_col   = pick(sqft_candidates,   ["sqft", "sq_ft", "heated_area", "living_area"])
    adom_col   = pick(adom_candidates,   ["adom", "dom"])
    beds_col   = pick(beds_candidates,   ["beds", "bedrooms"])
    baths_col  = pick(baths_candidates,  ["baths", "bathrooms"])

    return {
        "status_col": status_col,
        "type_col": type_col,
        "price_col": price_col,
        "sqft_col": sqft_col,
        "adom_col": adom_col,
        "beds_col": beds_col,
        "baths_col": baths_col,
    }


# ============================================================
# CLASSIFICATION (ACT/PND/SLD + words)
# ============================================================

_STATUS_MAP = {
    "SLD": "Sold",
    "SOLD": "Sold",
    "CLOSED": "Sold",
    "CLS": "Sold",

    "ACT": "Listings",
    "ACTIVE": "Listings",

    "PND": "Pending",
    "PENDING": "Pending",

    "RNT": "Rental",
    "RENT": "Rental",
    "RENTAL": "Rental",

    "LND": "Land",
    "LAND": "Land",
    "LOT": "Land",
}


def classify_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Cria coluna category:
      Sold / Listings / Pending / Rental / Land / Other
    Retorna (df_classificado, detected_cols)
    """
    df = df.copy()
    detected = detect_columns(df)

    status_col = detected["status_col"]
    type_col = detected["type_col"]

    # se não achar status, cria vazio
    if status_col is None:
        df["__status_text__"] = ""
    else:
        df["__status_text__"] = df[status_col].astype(str).fillna("").str.upper()

    if type_col is None:
        df["__type_text__"] = ""
    else:
        df["__type_text__"] = df[type_col].astype(str).fillna("").str.upper()

    def classify_value(status_text: str, type_text: str) -> str:
        t = f"{status_text} {type_text}".strip().upper()

        # tenta match por tokens “fortes”
        # 1) abreviações (SLD, ACT, PND, etc)
        for key, cat in _STATUS_MAP.items():
            # match de token inteiro (evita bater em pedaços)
            if re.search(rf"\b{re.escape(key)}\b", t):
                return cat

        # 2) fallback por palavras soltas
        if "SOLD" in t or "CLOSED" in t:
            return "Sold"
        if "ACTIVE" in t:
            return "Listings"
        if "PENDING" in t:
            return "Pending"
        if "RENT" in t:
            return "Rental"
        if "LAND" in t or "LOT" in t:
            return "Land"

        return "Other"

    df["category"] = [
        classify_value(st, tp)
        for st, tp in zip(df["__status_text__"], df["__type_text__"])
    ]

    return df, detected


# ============================================================
# METRICS
# ============================================================

def table_row_counts(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby("category")
        .size()
        .reset_index(name="rows")
        .sort_values("rows", ascending=False)
        .reset_index(drop=True)
    )


def weighted_avg(series, weights):
    series = pd.to_numeric(series, errors="coerce")
    weights = pd.to_numeric(weights, errors="coerce")
    mask = (series.notna()) & (weights.notna()) & (weights > 0)
    if not mask.any():
        return None
    return float((series[mask] * weights[mask]).sum() / weights[mask].sum())


def investor_grade_overview(df: pd.DataFrame, detected_cols: dict) -> pd.DataFrame:
    """
    - Ponderada onde faz sentido (price, $/sqft, adom, sqft, etc) usando sqft como peso (se existir)
    - Demais: média aritmética (quando não fizer sentido ponderar)
    """
    rows = []

    price_col = detected_cols.get("price_col")
    sqft_col = detected_cols.get("sqft_col")
    adom_col = detected_cols.get("adom_col")
    beds_col = detected_cols.get("beds_col")
    baths_col = detected_cols.get("baths_col")

    # peso preferido: sqft; se não existir, usa None
    weights = df[sqft_col] if sqft_col and sqft_col in df.columns else None

    def add_weighted(label, col):
        if not col or col not in df.columns:
            return
        if weights is None:
            val = pd.to_numeric(df[col], errors="coerce").mean()
            rows.append({"metric": label, "value": None if pd.isna(val) else float(val), "method": "mean (no sqft weight)"})
        else:
            val = weighted_avg(df[col], weights)
            rows.append({"metric": label, "value": val, "method": "weighted by sqft"})

    def add_mean(label, col):
        if not col or col not in df.columns:
            return
        val = pd.to_numeric(df[col], errors="coerce").mean()
        rows.append({"metric": label, "value": None if pd.isna(val) else float(val), "method": "mean"})

    # SOLD METRICS (ponderadas)
    add_weighted("Price", price_col)

    # $/sqft: se não existir coluna pronta, calcula se tiver price + sqft
    if price_col and sqft_col and price_col in df.columns and sqft_col in df.columns:
        p = pd.to_numeric(df[price_col], errors="coerce")
        s = pd.to_numeric(df[sqft_col], errors="coerce")
        df_calc = df.copy()
        df_calc["_ppsqft_calc"] = p / s
        add_weighted("$/Sqft", "_ppsqft_calc")

    add_weighted("ADOM", adom_col)
    add_weighted("Sqft", sqft_col)

    # Beds/Baths: por padrão média aritmética (como você pediu)
    add_mean("Beds (mean)", beds_col)
    add_mean("Baths (mean)", baths_col)

    return pd.DataFrame(rows)
