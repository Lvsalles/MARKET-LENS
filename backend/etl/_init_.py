"""
ETL Orchestrator — Market Lens (Cloud-first)

Responsabilidade:
- Orquestrar a ingestão
- Chamar o classificador determinístico (mls_classify.py)
- Persistir no banco (stg_mls_classified)

⚠️ NÃO contém regras de negócio
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import List, Optional

import pandas as pd

from backend.mls_classify import classify_xlsx
from backend.db.db import get_engine


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

CONTRACT_PATH = Path("backend/contracts/mls_column_contract.yaml")
TARGET_TABLE = "stg_mls_classified"

REQUIRED_COLUMNS = {
    "snapshot_date",
    "asset_class",
    "status_group",
    "ml_number",
    "list_price",
    "close_price",
}


def validate_dataframe(df: pd.DataFrame) -> None:
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"DataFrame inválido. Colunas faltando: {missing}")

    if df["ml_number"].isna().any():
        raise ValueError("Existem registros sem ML Number")


def persist_dataframe(df: pd.DataFrame) -> None:
    engine = get_engine()

    df.to_sql(
        TARGET_TABLE,
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )


def run_etl(
    xlsx_files: List[str],
    snapshot_date: Optional[date] = None,
    persist: bool = True,
) -> pd.DataFrame:

    if not CONTRACT_PATH.exists():
        raise FileNotFoundError(f"Contrato não encontrado: {CONTRACT_PATH}")

    snapshot_date = snapshot_date or date.today()

    all_dfs = []

    for file_path in xlsx_files:
        df = classify_xlsx(
            xlsx_path=file_path,
            contract_path=CONTRACT_PATH,
            snapshot_date=snapshot_date,
        )
        all_dfs.append(df)

    final_df = pd.concat(all_dfs, ignore_index=True)

    validate_dataframe(final_df)

    if persist:
        persist_dataframe(final_df)

    return final_df
