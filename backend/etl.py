# backend/etl.py
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd
from sqlalchemy import text, inspect
from sqlalchemy.engine import Engine

from backend.db import get_engine
from backend.contracts.mls_classify import classify_xlsx


@dataclass
class ETLResult:
    ok: bool
    import_id: str
    inserted_raw: bool
    inserted_classified_rows: int
    error: Optional[str] = None


# -----------------------------
# Helpers: schema-safe inserts
# -----------------------------

def _table_columns(engine: Engine, table_name: str, schema: str = "public") -> set[str]:
    insp = inspect(engine)
    cols = insp.get_columns(table_name, schema=schema)
    return {c["name"] for c in cols}


def _safe_insert_one(
    engine: Engine,
    table: str,
    payload: Dict[str, Any],
    conflict_cols: Optional[Tuple[str, ...]] = None,
    schema: str = "public",
) -> bool:
    """
    Inserts only keys that exist as real columns in Postgres.
    Returns True if executed (or would be a no-op due to conflict).
    """
    existing = _table_columns(engine, table, schema=schema)
    filtered = {k: v for k, v in payload.items() if k in existing}

    if not filtered:
        # Nothing to insert based on actual schema
        return False

    cols = list(filtered.keys())
    params = {k: filtered[k] for k in cols}

    col_sql = ", ".join(cols)
    val_sql = ", ".join([f":{c}" for c in cols])

    if conflict_cols:
        conflict_sql = f" ON CONFLICT ({', '.join(conflict_cols)}) DO NOTHING"
    else:
        conflict_sql = ""

    sql = text(f"INSERT INTO {schema}.{table} ({col_sql}) VALUES ({val_sql}){conflict_sql};")

    with engine.begin() as conn:
        conn.execute(sql, params)
    return True


def _safe_insert_many(
    engine: Engine,
    table: str,
    df: pd.DataFrame,
    schema: str = "public",
) -> int:
    """
    Bulk insert using only existing columns.
    Uses executemany with SQLAlchemy text.
    """
    if df.empty:
        return 0

    existing = _table_columns(engine, table, schema=schema)
    use_cols = [c for c in df.columns if c in existing]
    if not use_cols:
        return 0

    df2 = df[use_cols].copy()

    col_sql = ", ".join(use_cols)
    val_sql = ", ".join([f":{c}" for c in use_cols])

    sql = text(f"INSERT INTO {schema}.{table} ({col_sql}) VALUES ({val_sql});")

    records = df2.to_dict(orient="records")
    with engine.begin() as conn:
        conn.execute(sql, records)
    return len(records)


# -----------------------------
# Core ETL
# -----------------------------

def run_etl(
    xlsx_path: str | Path,
    *,
    contract_path: str | Path,
    snapshot_date: Optional[date] = None,
    engine: Optional[Engine] = None,
) -> ETLResult:
    """
    End-to-end:
      1) Create import_id (UUID)
      2) Insert into stg_mls_raw (schema-safe)
      3) Classify XLSX into DataFrame
      4) Add import_id to classified df
      5) Insert into stg_mls_classified (schema-safe)
    """
    try:
        engine = engine or get_engine()
        snapshot_date = snapshot_date or date.today()

        xlsx_path = Path(xlsx_path)
        contract_path = Path(contract_path)

        import_id = str(uuid.uuid4())

        # 1) Insert raw record (ONLY what exists in DB)
        inserted_raw = _safe_insert_one(
            engine,
            "stg_mls_raw",
            payload={
                "import_id": import_id,
                "filename": xlsx_path.name,
                "snapshot_date": snapshot_date,
                "imported_at": snapshot_date,  # if column exists & is date; otherwise ignored
            },
            conflict_cols=("import_id",),
        )

        # 2) Classify file
        df = classify_xlsx(xlsx_path=xlsx_path, contract_path=contract_path, snapshot_date=snapshot_date)

        # 3) Attach FK
        df["import_id"] = import_id

        # 4) Insert classified rows
        inserted_rows = _safe_insert_many(engine, "stg_mls_classified", df)

        return ETLResult(
            ok=True,
            import_id=import_id,
            inserted_raw=inserted_raw,
            inserted_classified_rows=inserted_rows,
        )

    except Exception as e:
        return ETLResult(
            ok=False,
            import_id="",
            inserted_raw=False,
            inserted_classified_rows=0,
            error=str(e),
        )
