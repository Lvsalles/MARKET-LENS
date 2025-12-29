import pandas as pd
from sqlalchemy import text


# ============================
# CHECA SE COLUNA EXISTE
# ============================
def table_has_column(engine, table_name: str, column_name: str) -> bool:
    query = text("""
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = :table
          AND column_name = :column
        LIMIT 1
    """)
    with engine.begin() as conn:
        result = conn.execute(query, {"table": table_name, "column": column_name}).fetchone()
    return result is not None


# ============================
# LEITURA SEGURA DO BANCO
# ============================
def read_stg(engine, project_id: str):
    has_project = table_has_column(engine, "stg_mls", "project_id")

    if has_project:
        sql = text("""
            SELECT *
            FROM stg_mls
            WHERE project_id = :project_id
        """)
        params = {"project_id": project_id}
    else:
        sql = text("SELECT * FROM stg_mls")
        params = {}

    with engine.begin() as conn:
        return pd.read_sql(sql, conn, params=params)


# ============================
# CLASSIFICAÇÃO DE STATUS
# ============================
def classify_rows(df: pd.DataFrame) -> pd.DataFrame:
    def classify(row):
        txt = " ".join([
            str(row.get("status", "")),
            str(row.get("property_type", "")),
        ]).upper()

        if "SOLD" in txt or "CLOSED" in txt:
            return "Sold"
        if "ACTIVE" in txt:
            return "Listings"
        if "PENDING" in txt:
            return "Pending"
        if "RENT" in txt:
            return "Rental"
        if "LAND" in txt:
            return "Land"
        return "Other"

    df = df.copy()
    df["category"] = df.apply(classify, axis=1)
    return df


# ============================
# MÉTRICAS
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
    results = []

    def add(metric, col):
        if col not in df.columns:
            return
        val = weighted_avg(df[col], df.get("sqft"))
        results.append({"metric": metric, "weighted_avg": val})

    add("Sold Price", "sold_price")
    add("Price per Sqft", "ppsqft")
    add("ADOM", "adom")
    add("Beds", "beds")
    add("Baths", "baths")

    return pd.DataFrame(results)
