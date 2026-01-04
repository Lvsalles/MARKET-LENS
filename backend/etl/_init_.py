"""
ETL Orchestrator — Market Lens (Cloud-first)

Este módulo DEFINE e EXPORTA run_etl.
"""

from datetime import date
from pathlib import Path
from typing import List, Optional

import pandas as pd

from backend.core.mls_classify import classify_xlsx
from backend.db.db import get_engine

# caminho do contrato
CONTRACT_PATH = Path("backend/contracts/mls_column_contract.yaml")

# tabela de destino
TARGET_TABLE = "stg_mls_classified"


def run_etl(
    xlsx_files: List[str],
    snapshot_date: Optional[date] = None,
    persist: bool = True,
) -> pd.DataFrame:
    """
    Executa o ETL completo:
    - classifica XLSX
    - concatena
    - persiste no banco
    """

    snapshot_date = snapshot_date or date.today()
    dfs = []

    for file_path in xlsx_files:
        df = classify_xlsx(
            xlsx_path=file_path,
            contract_path=CONTRACT_PATH,
            snapshot_date=snapshot_date,
        )
        dfs.append(df)

    if not dfs:
        raise ValueError("Nenhum arquivo XLSX foi processado.")

    final_df = pd.concat(dfs, ignore_index=True)

    if persist:
        engine = get_engine()
        final_df.to_sql(
            TARGET_TABLE,
            engine,
            if_exists="append",
            index=False,
            method="multi",
        )

    return final_df


# export explícito (boa prática)
__all__ = ["run_etl"]
