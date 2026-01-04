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
from backend.contract.mls_classify import classify_xlsx   # ✅ PATH CORRETO


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
    use_cols = [c for c in df.columns if c in existing_cols]

    if not use_cols:
        return 0

    df = df[use_cols].copy()

    cols_sql = ", ".join(use_cols)
    vals_sql = ", ".join(f":{c}" for c in use_cols)

    sql = text(
        f"""
        INSERT INTO {schema}.{table} ({cols_sql})
        VALUES ({vals_sql});
        """
    )

    with engine.begin() as conn:
        conn.execute(sql, df.to_dict(orient="records"))

    return len(df)


# =========================================================
# ETL PRINCIPAL — FINAL
# =========================================================

def run_etl(
    xlsx_path: str | Path,
    *,
    contract_path: str | Path,
    snapshot_date: Optional[date] = None,
    engine: Optional[Engine] = None,
) -> ETLResult:

    try:
        engine = engine or get_engine()
        snapshot_date = snapshot_date or date.today()

        xlsx_path = Path(xlsx_path)
        contract_path = Path(contract_path)

        if not xlsx_path.exists():
            raise FileNotFoundError(f"XLSX não encontrado: {xlsx_path}")

        if not contract_path.exists():
            raise FileNotFoundError(f"Contract YAML não encontrado: {contract_path}")

        import_id = str(uuid.uuid4())

        # -------------------------------------------------
        # 1) RAW IMPORT (contrato REAL do banco)
        # -------------------------------------------------
        inserted_raw = _safe_insert_one(
            engine,
            table="stg_mls_raw",
            payload={
                "import_id": import_id,
                "source_file": xlsx_path.name,  # 🔴 OBRIGATÓRIO
            },
            conflict_cols=("import_id",),
        )

        # -------------------------------------------------
        # 2) Classificação MLS
        # -------------------------------------------------
        df = classify_xlsx(
            xlsx_path=xlsx_path,
            contract_path=contract_path,
            snapshot_date=snapshot_date,
        )

        # -------------------------------------------------
        # 3) FK
        # -------------------------------------------------
        df["import_id"] = import_id

        # -------------------------------------------------
        # 4) Insert classificados
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
