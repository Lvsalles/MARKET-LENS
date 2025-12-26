import yaml
import pandas as pd
from semantic_dictionary import normalize_financing

with open("column_dictionary.yaml") as f:
    COLUMN_MAP = yaml.safe_load(f)

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for canonical, variants in COLUMN_MAP.items():
        if isinstance(variants, list):
            for v in variants:
                if v in df.columns:
                    df[canonical] = df[v]
                    break
        elif canonical in df.columns:
            continue

    if "acreage" in df.columns and "area_sqft" not in df.columns:
        df["area_sqft"] = df["acreage"] * 43560

    if "buyer_financing" in df.columns:
        df["buyer_financing"] = df["buyer_financing"].apply(normalize_financing)

    return df
