import pandas as pd
from sqlalchemy import text


# =========================================
# UTILIDADES
# =========================================

def table_has_column(engine, table_name: str, column_name: str) -> bool:
    query = text("""
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = :table
          AND column_name = :column
        LIMIT 1
    """)
    with engine.begin() as conn:
        res = conn.execute(query, {"table": table_name, "column": column_name}).fetchone()
    return res is not None


# =========================================
# LEITURA SEGURA DO BANCO
# =========================================

def read_stg(engine, project_id: str) -> pd.DataFrame:
    """
    Lê dados da tabela stg_mls.
    Sempre retorna um DataFrame válido (mesmo vazio).
    """
    try:
        has_project = table_has_column(engine, "stg_mls", "project_id")

        if has_project:
            sql = text("SELECT * FROM stg_mls WHERE project_id = :project_id")
            params = {"project_id": project_id}
        else:
            sql = text("SELECT * FROM stg_mls")
            params = {}

        with engine.begin() as conn:
            df = pd.read_sql(sql, conn, params=params)

        if df is None:
            return pd.DataFrame()

        return df

    except Exception as e:
        print("ERRO AO LER stg_mls:", str(e))
        return pd.DataFrame()


# =========================================
# CLASSIFICAÇÃO DE STATUS
# =========================================

def classify_rows(df: pd.DataFrame):
    if df is None or df.empty:
        return pd.DataFrame(), {}

    df = df.copy()

    # detectar colunas
    def find_col(keys):
        for c in df.columns:
            for k in keys:
                if k in c.lower():
                    return c
        return None

    status_col = find_col(["status"])
    type_col = find_col(["type", "property"])

    def classify(row):
        text = f"{str(row.get(status_col, ''))} {str(row.get(type_col, ''))}".upper()
        if "SOLD" in text or "CLOSED" in text:
            return "Sold"
        if "ACTIVE" in text:
            return "Listings"
        if "PENDING" in text:
            return "Pending"
        if "RENT" in text:
            return "Rental"
        if "LAND" in text:
            return "Land"
        return "Other"

    df["category"] = df.apply(classify, axis=1)

    return df, {
        "status_col": status_col,
        "type_col": type_col
    }


# =========================================
# CONTAGENS
# =========================================

def table_row_counts(df: pd.DataFrame):
    if df is None or df.empty:
        return pd.DataFrame(columns=["category", "rows"])

    return (
        df.groupby("category")
        .size()
        .reset_index(name="rows")
        .sort_values("rows", ascending=False)
    )
