from __future__ import annotations

import uuid
from pathlib import Path
from datetime import date

import pandas as pd
from sqlalchemy import text

from backend.db import get_engine
from backend.contracts.mls_classify import classify_xlsx


def run_etl(
    xlsx_path: str | Path,
    contract_path: str | Path,
    snapshot_date: date,
) -> None:
    engine = get_engine()

    # 🔑 UM UUID POR EXECUÇÃO
    import_id = uuid.uuid4()

    # =========================
    # 1. RAW
    # =========================
    raw_df = pd.read_excel(xlsx_path, engine="openpyxl")
    raw_df["import_id"] = import_id
    raw_df["snapshot_date"] = snapshot_date

    raw_df.to_sql(
        "stg_mls_raw",
        engine,
        if_exists="append",
        index=False,
        method="multi",
    )

    # =========================
    # 2. CLASSIFIED
    # =========================
    classified_df = classify_xlsx(
        xlsx_path=xlsx_path,
        contract_path=contract_path,
        snapshot_date=snapshot_date,
    )

    classified_df["import_id"] = import_id

    classified_df.to_sql(
        "stg_mls_classified",
        engine,
        if_exists="append",
        index=False,
        method="multi",
    )
