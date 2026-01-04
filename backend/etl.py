# backend/etl.py
from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from backend.db import get_engine
from backend.contract.mls_classify import classify_xlsx


# =========================================================
# Resultado do ETL
# =========================================================

@dataclass
class ETLResult:
    ok: bool
    import_id: str
    inserted_raw: bool
    inserted_classified_rows: int
    error: Optional[str] = None


# =========================================================
# Helpers — schema-aware
# =========================================================

def _get_table_columns(engine: Engine, table: str, schema: str = "public") -> set[str]:
    inspector = inspect(engine)
    return {col["name"] for col in inspector.get_columns(table, schema=schema)}


def _safe_insert_one(
    engine: Engine,
    table: str,
    payload: Dict[str, Any],
    conflict_cols: Optional[tuple[str, ...]] = None,
    schema: str = "public",
) -> bool:
    existing_cols = _get_table_columns(engine, table, schema)
    data = {k: v for k, v in payload.items() if k in existing_cols}

    if not data:
        return False

    cols_sql = ", ".join(data.keys())
    vals_sql = ", ".join(f":{k}" for k in data.keys())

    conflict_sql = ""
    if conflict_cols:
        conflict_sql = f" ON CONFLICT ({', '.join(conflict_cols)}) DO NOTHING"

    sql = text(
        f"""
        INSERT INTO {schema}.{table} ({cols_sql})
        VALUES ({vals_sql})
        {conflict_sql};
        """
    )

    with engine.begin() as conn:
        conn.execute(sql, data)

    return True


def _safe_insert_many(
    engine: Engine,
    table: str,
    df: pd.DataFrame,
    schema: str = "public",
) -> int:
    if df.empty:
        return 0

    existing_cols = _get_table_columns(engine, table, schema)
    use_cols = [c f]()_
