import pandas as pd
from sqlalchemy import text
from db import get_engine


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # padroniza nomes
    df.columns = (
        df.columns
        .str.lower()
        .str.strip()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )

    # mapeamentos comuns
    rename_map = {
        "mls": "mls_id",
        "list_price": "list_price",
        "sold_price": "sold_price",
        "price": "list_price",
        "beds": "beds",
        "baths": "baths",
        "sqft": "sqft",
        "living_area": "sqft",
        "address": "address",
        "street": "street",
        "city": "city",
        "zip": "zipcode",
        "zip_code": "zipcode",
        "year_built": "year_built",
        "dom": "dom",
        "adom": "adom",
        "list_date": "list_date",
        "sold_date": "sold_date",
        "agent": "list_agent",
        "listing_agent": "list_agent",
        "selling_agent": "sell_agent"
    }

    df = df.rename(columns={c: rename_map[c] for c in df.columns if c in rename_map})
    return df


def insert_into_staging(df: pd.DataFrame, project_id: str, category: str):
    engine = get_engine()

    df["project_id"] = project_id
    df["category"] = category

    df.to_sql(
        "stg_mls",
        engine,
        if_exists="append",
        index=False,
        method="multi"
    )
