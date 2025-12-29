import pandas as pd
from sqlalchemy import text


# ===============================
# LEITURA SEGURA DO BANCO
# ===============================
def read_stg(engine, project_id: str) -> pd.DataFrame:
    try:
        query = text("""
            SELECT *
            FROM stg_mls
            WHERE project_id = :project_id
        """)
        with engine.begin() as conn:
            df = pd.read_sql(query, conn, params={"project_id": project_id})
        return df

    except Exception as e:
        print("ERRO AO LER STG:", e)
        return pd.DataFrame()


# ===============================
# CLASSIFICAÇÃO DE STATUS
# ===============================
def classify_rows(df: pd.DataFrame):
    if df.empty:
        return df, {}

    def normalize(x):
        return str(x).strip().upper()

    status_col = None
    for c in df.columns:
        if "status" in c.lower():
            status_col = c
            break

    def classify(row):
        s = normalize(row.get(status_col, ""))

        if "SOLD" in s or "CLOSED" in s:
            return "Sold"
        if "ACTIVE" in s:
            return "Listings"
        if "PENDING" in s:
            return "Pending"
        if "RENT" in s:
            return "Rental"
        if "LAND" in s:
            return "Land"
        return "Other"

    df["category"] = df.apply(classify, axis=1)

    return df, {
        "status_col": status_col
    }


# ===============================
# MÉTRICAS SIMPLES
# ===============================
def table_row_counts(df: pd.DataFrame):
    if df is None or df.empty:
        return pd.DataFrame(columns=["category", "rows"])

    return (
        df.groupby("category")
        .size()
        .reset_index(name="rows")
        .sort_values("rows", ascending=False)
    )
