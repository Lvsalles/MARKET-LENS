import pandas as pd
from sqlalchemy import text

# ============================
# LEITURA DO STAGING
# ============================
def read_stg(engine, project_id: str, category: str | None = None):
    base_query = """
        SELECT *
        FROM stg_mls
        WHERE project_id = :project_id
    """

    if category:
        if category == "Sold":
            base_query += " AND status ILIKE '%SOLD%'"
        elif category == "Listings":
            base_query += " AND status ILIKE '%ACTIVE%'"
        elif category == "Pending":
            base_query += " AND status ILIKE '%PENDING%'"
        elif category == "Rental":
            base_query += " AND status ILIKE '%RENT%'"
        elif category == "Land":
            base_query += " AND property_type ILIKE '%LAND%'"

    with engine.begin() as conn:
        return pd.read_sql(base_query, conn, params={"project_id": project_id})


# ============================
# MÉTRICAS
# ============================

def table_row_counts(engine, project_id):
    q = """
        SELECT status, COUNT(*) AS total
        FROM stg_mls
        WHERE project_id = :project_id
        GROUP BY status
        ORDER BY total DESC
    """
    with engine.begin() as conn:
        return pd.read_sql(q, conn, params={"project_id": project_id})


def weighted_avg(series, weights):
    series = pd.to_numeric(series, errors="coerce")
    weights = pd.to_numeric(weights, errors="coerce")
    mask = (series.notna()) & (weights.notna()) & (weights > 0)
    if mask.sum() == 0:
        return None
    return (series[mask] * weights[mask]).sum() / weights[mask].sum()


def investor_grade_overview(df):
    if df.empty:
        return pd.DataFrame()

    results = []

    def add(metric, col):
        if col not in df.columns:
            return
        w = df["sqft"] if "sqft" in df.columns else None
        value = weighted_avg(df[col], w)
        results.append({
            "metric": metric,
            "weighted_avg": value
        })

    add("Sold Price", "sold_price")
    add("Price per Sqft", "ppsqft")
    add("ADOM", "adom")
    add("Beds", "beds")
    add("Baths", "baths")

    return pd.DataFrame(results)
