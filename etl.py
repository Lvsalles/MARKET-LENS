import pandas as pd
import hashlib
from sqlalchemy import text

def normalize_columns(df):
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    return df

def build_natural_key(row):
    base = "|".join(str(v) for v in row.values)
    return hashlib.md5(base.encode()).hexdigest()

def insert_staging(engine, df, project_id, dataset_type):
    df = normalize_columns(df)

    if "status" not in df.columns:
        df["status"] = None

    df["natural_key"] = df.apply(build_natural_key, axis=1)

    rows = []
    for _, r in df.iterrows():
        rows.append({
            "project_id": project_id,
            "dataset_type": dataset_type,
            "status": r.get("status"),
            "data": r.drop(["natural_key"]).to_dict(),
            "natural_key": r["natural_key"]
        })

    sql = text("""
        insert into stg_raw (project_id, dataset_type, status, data, natural_key)
        values (:project_id, :dataset_type, :status, :data::jsonb, :natural_key)
        on conflict (project_id, dataset_type, natural_key) do nothing
    """)

    with engine.begin() as conn:
        conn.execute(sql, rows)

    return len(rows)
