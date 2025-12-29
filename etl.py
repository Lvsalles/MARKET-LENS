from __future__ import annotations
import io
import uuid
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from normalization import normalize_mls_dataframe


def ensure_schema(engine: Engine) -> None:
    """
    Garante que as tabelas mínimas existem.
    (Você vai rodar schema.sql uma vez no Supabase; aqui é só “safety check”.)
    """
    with engine.begin() as conn:
        conn.execute(text("select 1"))


def read_excel(file_bytes: bytes) -> pd.DataFrame:
    bio = io.BytesIO(file_bytes)
    df = pd.read_excel(bio)
    return df


def create_project_if_missing(engine: Engine, owner_id: str, name: str) -> str:
    with engine.begin() as conn:
        row = conn.execute(
            text("select id from projects where owner_id=:o and name=:n limit 1"),
            {"o": owner_id, "n": name},
        ).fetchone()
        if row:
            return str(row[0])

        pid = conn.execute(
            text("insert into projects(owner_id, name) values(:o,:n) returning id"),
            {"o": owner_id, "n": name},
        ).fetchone()[0]
        return str(pid)


def create_dataset(engine: Engine, project_id: str, filename: str, category: str) -> str:
    dataset_id = str(uuid.uuid4())
    with engine.begin() as conn:
        conn.execute(
            text("""
                insert into datasets(id, project_id, filename, category, status)
                values (:id, :project_id, :filename, :category, 'processing')
            """),
            {"id": dataset_id, "project_id": project_id, "filename": filename, "category": category},
        )
    return dataset_id


def insert_normalized(engine: Engine, project_id: str, dataset_id: str, norm: pd.DataFrame) -> int:
    norm = norm.copy()
    norm["project_id"] = project_id
    norm["dataset_id"] = dataset_id

    cols = [
        "project_id","dataset_id","category",
        "mls_id","address","city","zipcode",
        "property_type","property_subtype","financing",
        "price","sold_price","sqft",
        "beds","baths","garage",
        "dom","adom",
        "list_date","sold_date",
        "ppsqft","month_key",
        "list_agent","sell_agent"
    ]

    for c in cols:
        if c not in norm.columns:
            norm[c] = None

    records = norm[cols].to_dict(orient="records")

    with engine.begin() as conn:
        conn.execute(
            text(f"""
                insert into normalized_properties (
                    {",".join(cols)}
                ) values (
                    {",".join([f":{c}" for c in cols])}
                )
            """),
            records,
        )

    with engine.begin() as conn:
        conn.execute(
            text("update datasets set status='completed', record_count=:c where id=:id"),
            {"c": len(records), "id": dataset_id},
        )

    return len(records)


def ingest_excel(engine: Engine, owner_id: str, project_name: str, category: str, filename: str, file_bytes: bytes) -> dict:
    ensure_schema(engine)
    project_id = create_project_if_missing(engine, owner_id, project_name)
    dataset_id = create_dataset(engine, project_id, filename, category)

    df = read_excel(file_bytes)
    norm = normalize_mls_dataframe(df, category=category)

    inserted = insert_normalized(engine, project_id, dataset_id, norm)

    return {"project_id": project_id, "dataset_id": dataset_id, "rows": int(len(df)), "inserted": int(inserted)}
