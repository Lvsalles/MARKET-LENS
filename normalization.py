import yaml
import pandas as pd
from schema_dictionary import SCHEMAS

with open("column_dictionary.yaml") as f:
    COLUMN_MAP = yaml.safe_load(f)

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip().lower() for c in df.columns]

    rename_map = {}
    for canonical, variants in COLUMN_MAP.items():
        for v in variants:
            if v.lower() in df.columns:
                rename_map[v.lower()] = canonical

    return df.rename(columns=rename_map)

def enforce_schema(df: pd.DataFrame, schema_name: str) -> pd.DataFrame:
    schema = SCHEMAS[schema_name]

    for col in schema:
        if col not in df.columns:
            df[col] = None

    return df[schema]
