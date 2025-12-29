import pandas as pd
from sqlalchemy import text

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("/", "_")
    )
    return df


def detect_category(filename: str) -> str:
    name = filename.lower()
    if "land" in name:
        return "land"
    if "rent" in name or "rental" in name:
        return "rental"
    return "properties"


def load_excel_to_db(engine, filepath: str, project_id: str):
    df = pd.read_excel(filepath)
    df = normalize_columns(df)

    category = detect_category(filepath)
    df["category"] = category
    df["project_id"] = project_id

    # Mapeamento defensivo
    rename_map = {
        "list_price": "list_price",
        "price": "list_price",
        "sold_price": "sold_price",
        "close_price": "sold_price",
        "beds": "beds",
        "bedrooms": "beds",
        "baths": "full_baths",
        "bathrooms": "full_baths",
        "sqft": "sqft",
        "living_area": "sqft",
        "year_built": "year_built",
        "status": "status",
        "address": "address",
        "city": "city",
        "zip": "zipcode",
        "mls_number": "ml_number"
    }

    df.rename(columns=rename_map, inplace=True)

    keep_cols = [
        "project_id", "category", "ml_number", "status",
        "address", "city", "zipcode",
        "beds", "full_baths", "sqft",
        "year_built", "list_price", "sold_price"
    ]

    df = df[[c for c in keep_cols if c in df.columns]]

    df.to_sql("stg_mls", engine, if_exists="append", index=False)
