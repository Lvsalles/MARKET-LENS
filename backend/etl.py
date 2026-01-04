from __future__ import annotations

import uuid
from datetime import date
from pathlib import Path
from typing import Optional, Dict, Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from backend.db import get_engine
from backend.contract.mls_classify import classify_xlsx


# =========================================================
# ETL PRINCIPAL — DEFINITIVO
# =========================================================

def run_etl(
    xlsx_path: str | Path,
    *,
    contract_path: str | Path,
    snapshot_date: Optional[date] = None,
    source_tag: str = "MLS",
    engine: Optional[Engine] = None,
) -> dict:

    engine = engine or get_engine()
    snapshot_date = snapshot_date or date.today()

    xlsx_path = Path(xlsx_path)
    contract_path = Path(contract_path)

    if not xlsx_path.exists():
        raise FileNotFoundError(f"XLSX não encontrado: {xlsx_path}")

    if not contract_path.exists():
        raise FileNotFoundError(f"Contract YAML não encontrado: {contract_path}")

    import_id = str(uuid.uuid4())

    # =====================================================
    # 1) Ler XLSX bruto
    # =====================================================
    raw_df = pd.read_excel(xlsx_path, engine="openpyxl")

    if raw_df.empty:
        raise ValueError("Arquivo XLSX está vazio")

    # =====================================================
    # 2) Insert RAW — UMA LINHA POR ROW
    # =====================================================
    raw_records = []
    for idx, row in raw_df.iterrows():
        raw_records.append(
            {
                "import_id": import_id,
                "source_file": xlsx_path.name,
                "source_tag": source_tag,
                "row_number": idx + 1,   # 🔴 FIX DEFINITIVO
                "snapshot_date": snapshot_date,
            }
        )

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO public.stg_mls_raw
                (import_id, source_file, source_tag, row_number, snapshot_date)
                VALUES
                (:import_id, :source_file, :source_tag, :row_number, :snapshot_date)
                """
            ),
            raw_records,
        )

    # =====================================================
    # 3) Classificação
    # =====================================================
    classified_df = classify_xlsx(
        xlsx_path=xlsx_path,
        contract_path=contract_path,
        snapshot_date=snapshot_date,
    )

    classified_df["import_id"] = import_id

    # =====================================================
    # 4) Insert classificados
    # =====================================================
    with engine.begin() as conn:
        classified_df.to_sql(
            "stg_mls_classified",
            conn,
            schema="public",
            if_exists="append",
            index=False,
            method="multi",
        )

    return {
        "ok": True,
        "import_id": import_id,
        "raw_rows": len(raw_records),
        "classified_rows": len(classified_df),
    }
