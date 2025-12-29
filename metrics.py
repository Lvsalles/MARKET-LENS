import pandas as pd
from sqlalchemy import text


# ===========================
# UTILS
# ===========================

def table_has_column(engine, table_name: str, column_name: str) -> bool:
    query = """
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = :table
          AND column_name = :column
        LIMIT 1
    """
    with engine.begin() as conn:
        res = conn.execute(query, {"table": table_name, "column": column_name}).fetchone()
    return res is not None


# ===========================
# LOAD DATA SAFELY
# ===========================

def read_stg(engine, project_id: str):
    """
    Carrega dados da stg_mls com fallback automático
    se a coluna project_id não existir.
    """

    has_project_id = table_has_column(engine, "stg_mls", "project_id")

    if has_project_id:
        sql = """
            SELECT *
            FROM stg_mls
            WHERE project_id = :project_id
        """
        params = {"project_id": project_id}
    else:
        # fallback — tabela não possui project_id
        sql = "SELECT * FROM stg_mls"
        params = {}

    with engine.begin() as conn:
        return pd.read_sql(sql, conn, params=params)


# ===========================
# CLASSIFICAÇÃO DE STATUS
# ===========================

def classify_rows(df: pd.DataFrame) -> pd.DataFrame:
    def classify(row):
        text = " ".join([
            str(row.get("status", "")),
            str(row.get("property_type", "")),
        ]).upper()

        if "SOLD" in text or "CLOSED" in text:
            return "Sold"
        if "ACTIVE" in text:
            return "Listings"
        if "PENDING" in text:
            return "Pending"
        if "RENT" in text:
            return "Rental"
        if "LAND" in text or "LOT" in text:
            return "Land"
        return "Other"

    df = df.copy()
    df["category"] = df.apply(classify, axis=1)
    return df


# ===========================
# COUNTS
# ===========================

def table_row_counts(df: pd.DataFrame):
    return (
        df.groupby("category")
        .size()
        .reset_index(name="rows")
        .sort_values("rows", ascending=False)
    )


# ===========================
# METRICS
# ===========================

def weighted_avg(series, weights):
    series = pd.to_numeric(series, errors="coerce")
    weights = pd.to_numeric(weights, errors="coerce")

    mask = (series.notna()) & (weights.notna()) & (weights > 0)
    if not mask.any():
        return None

    return (series[mask] * weights[mask]).sum() / weights[mask].sum()


def investor_grade_overview(df: pd.DataFrame):
    metrics = []

    def add(name, col):
        if col in df.columns:
            val = weighted_avg(df[col], df.get("sqft"))
            metrics.append({
                "metric": name,
                "weighted_avg": val
            })

    add("Sold Price", "sold_price")
    add("Price per Sqft", "ppsqft")
    add("ADOM", "adom")
    add("Beds", "beds")
    add("Baths", "baths")

    return pd.DataFrame(metrics)
