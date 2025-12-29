import pandas as pd

# ============================
# LOAD DATA (SAFE)
# ============================

def read_stg(engine, project_id: str):
    query = """
        SELECT *
        FROM stg_mls
        WHERE project_id = :project_id
    """
    with engine.begin() as conn:
        return pd.read_sql(query, conn, params={"project_id": project_id})


# ============================
# CLASSIFICATION LOGIC
# ============================

def classify_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria coluna 'category' a partir de texto livre (status, property_type etc)
    """
    def classify(row):
        txt = " ".join(
            str(v).upper()
            for v in [
                row.get("status", ""),
                row.get("property_type", ""),
                row.get("property_sub_type", "")
            ]
        )

        if "SOLD" in txt or "CLOSED" in txt:
            return "Sold"
        if "ACTIVE" in txt:
            return "Listings"
        if "PENDING" in txt:
            return "Pending"
        if "RENT" in txt:
            return "Rental"
        if "LAND" in txt or "LOT" in txt:
            return "Land"
        return "Other"

    df = df.copy()
    df["category"] = df.apply(classify, axis=1)
    return df


# ============================
# METRICS
# ============================

def table_row_counts(df: pd.DataFrame):
    return (
        df.groupby("category")
        .size()
        .reset_index(name="rows")
        .sort_values("rows", ascending=False)
    )


def weighted_avg(series, weights):
    series = pd.to_numeric(series, errors="coerce")
    weights = pd.to_numeric(weights, errors="coerce")

    mask = (series.notna()) & (weights.notna()) & (weights > 0)
    if not mask.any():
        return None

    return (series[mask] * weights[mask]).sum() / weights[mask].sum()


def investor_grade_overview(df: pd.DataFrame):
    rows = []

    def add(label, col):
        if col not in df.columns:
            return
        val = weighted_avg(df[col], df.get("sqft"))
        rows.append({
            "metric": label,
            "weighted_avg": val
        })

    add("Sold Price", "sold_price")
    add("Price per Sqft", "ppsqft")
    add("ADOM", "adom")
    add("Beds", "beds")
    add("Baths", "baths")

    return pd.DataFrame(rows)
