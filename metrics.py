import pandas as pd
from sqlalchemy import text

# ============================================================
# LEITURA BASE
# ============================================================

def read_stg(engine, project_id: str, category: str | None = None):
    """
    Lê dados da tabela stg_mls e filtra por categoria lógica
    (Sold / Listings / Pending / Rental / Land)
    """
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


# ============================================================
# CONTAGEM POR STATUS
# ============================================================

def table_row_counts(engine, project_id: str):
    """
    Retorna quantidade de registros por tipo (Sold, Listings, etc)
    baseado em STATUS.
    """
    query = """
        SELECT
            CASE
                WHEN status ILIKE '%SOLD%' THEN 'Sold'
                WHEN status ILIKE '%CLOSED%' THEN 'Sold'
                WHEN status ILIKE '%ACTIVE%' THEN 'Listings'
                WHEN status ILIKE '%PENDING%' THEN 'Pending'
                WHEN status ILIKE '%RENT%' THEN 'Rental'
                ELSE 'Other'
            END AS category,
            COUNT(*) AS total
        FROM stg_mls
        WHERE project_id = :project_id
        GROUP BY 1
        ORDER BY total DESC
    """
    with engine.begin() as conn:
        return pd.read_sql(query, conn, params={"project_id": project_id})


# ============================================================
# MÉTRICAS (PONDERADAS)
# ============================================================

def weighted_average(values, weights):
    values = pd.to_numeric(values, errors="coerce")
    weights = pd.to_numeric(weights, errors="coerce")

    mask = (values.notna()) & (weights.notna()) & (weights > 0)
    if mask.sum() == 0:
        return None

    return (values[mask] * weights[mask]).sum() / weights[mask].sum()


def investor_grade_overview(df: pd.DataFrame):
    if df.empty:
        return pd.DataFrame()

    results = []

    def add(metric, col):
        if col not in df.columns:
            return
        value = weighted_average(df[col], df.get("sqft"))
        results.append({
            "metric": metric,
            "weighted_avg": round(value, 2) if value else None
        })

    add("Sold Price", "sold_price")
    add("Price per Sqft", "ppsqft")
    add("ADOM", "adom")
    add("Beds", "beds")
    add("Baths", "baths")

    return pd.DataFrame(results)
