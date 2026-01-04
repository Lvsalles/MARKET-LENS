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
from backend.contracts.mls_classify import classify_xlsx


# =========================================================
# Result object
# =========================================================

@dataclass
class ETLResult:
    ok: bool
    import_id: str
    inserted_raw: bool
    inserted_classified_rows: int
    error: Optional[str] = None


# =========================================================
# Helpers — schema-aware (NO ASSUMPTIONS)
# =========================================================

def _get_table_columns(engine: Engine, table_name: str, schema: str = "public") -> set[str]:
    inspector = inspect(engine)
    cols = inspector.get_columns(table_name, schema=schema)
    return {c["name"] for c in cols}


def _safe_insert_one(
    engine: Engine,
    table: str,
    payload: Dict[str, Any],
    conflict_cols: Optional[tuple[str, ...]] = None,
    schema: str = "public",
) -> bool:
    """
    Inserts a single row using ONLY columns that actually exist in Postgres.
    """
    existing_cols = _get_table_columns(engine, table, schema)
    filtered = {k: v for k, v in payload.items() if k in existing_cols}

    if not filtered:
        return False

    columns_sql = ", ".join(filtered.keys())
    values_sql = ", ".join(f":{k}" for k in filtered.keys())

    conflict_sql = ""
    if conflict_cols:
        conflict_sql = f" ON CONFLICT ({', '.join(conflict_cols)}) DO NOTHING"

    sql = text(
        f"""
        INSERT INTO {schema}.{table} ({columns_sql})
        VALUES ({values_sql})
        {conflict_sql};
        """
    )

    with engine.begin() as conn:
        conn.execute(sql, filtered)

    return True


def _safe_insert_many(
    engine: Engine,
    table: str,
    df: pd.DataFrame,
    schema: str = "public",
) -> int:
    """
    Bulk insert DataFrame using only existing columns.
    """
    if df.empty:
        return 0

    existing_cols = _get_table_columns(engine, table, schema)
    usable_cols = [c for c in df.columns if c in existing_cols]

    if not usable_cols:
        return 0

    df2 = df[usable_cols].copy()

    columns_sql = ", ".join(usable_cols)
    values_sql = ", ".join(f":{c}" for c in usable_cols)

    sql = text(
        f"""
        INSERT INTO {schema}.{table} ({columns_sql})
        VALUES ({values_sql});
        """
    )

    records = df2.to_dict(orient="records")

    with engine.begin() as conn:
        conn.execute(sql, records)

    return len(records)


# =========================================================
# Main ETL — FINAL, CONTRACT-ALIGNED
# =========================================================

def run_etl(
    xlsx_path: str | Path,
    *,
    contract_path: str | Path,
    snapshot_date: Optional[date] = None,
    engine: Optional[Engine] = None,
) -> ETLResult:
    """
    End-to-end MLS ETL

    Steps:
    1) Generate import_id (UUID)
    2) Insert into stg_mls_raw (REQUIRED: source_file)
    3) Classify XLSX via contract
    4) Attach import_id
    5) Insert into stg_mls_classified
    """

    try:
        engine = engine or get_engine()
        snapshot_date = snapshot_date or date.today()

        xlsx_path = Path(xlsx_path)
        contract_path = Path(contract_path)

        if not xlsx_path.exists():
            raise FileNotFoundError(f"XLSX file not found: {xlsx_path}")

        if not contract_path.exists():
            raise FileNotFoundError(f"Contract not found: {contract_path}")

        # -------------------------------------------------
        # 1) Generate import_id
        # -------------------------------------------------
        import_id = str(uuid.uuid4())

        # -------------------------------------------------
        # 2) Insert RAW import (REAL DB CONTRACT)
        # REQUIRED by DB:
        #   - import_id
        #   - source_file (NOT NULL)
        # -------------------------------------------------
        inserted_raw = _safe_insert_one(
            engine,
            table="stg_mls_raw",
            payload={
                "import_id": import_id,
                "source_file": xlsx_path.name,  # 🔴 REQUIRED
            },
            conflict_cols=("import_id",),
        )

        # -------------------------------------------------
        # 3) Classify XLSX
        # -------------------------------------------------
        df = classify_xlsx(
            xlsx_path=xlsx_path,
            contract_path=contract_path,
            snapshot_date=snapshot_date,
        )

        # -------------------------------------------------
        # 4) Attach FK
        # -------------------------------------------------
        df["import_id"] = import_id

        # -------------------------------------------------
        # 5) Insert classified rows
        # -------------------------------------------------
        inserted_rows = _safe_insert_many(
            engine,
            table="stg_mls_classified",
            df=df,
        )

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
