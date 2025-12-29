import pandas as pd

# Colunas que EXISTEM no banco
ALLOWED_COLUMNS = {
    "ml_number",
    "status",
    "address",
    "city",
    "zipcode",
    "legal_subdivision_name",
    "heated_area",
    "current_price",
    "beds",
    "full_baths",
    "half_baths",
    "year_built",
    "pool"
}

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )

    # manter apenas colunas válidas
    df = df[[c for c in df.columns if c in ALLOWED_COLUMNS]]

    return df
