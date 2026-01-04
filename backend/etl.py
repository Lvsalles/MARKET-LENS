from datetime import date
from pathlib import Path
from typing import List, Optional
from uuid import uuid4

import pandas as pd

from backend.core.mls_classify import classify_xlsx
from backend.db.db import get_engine

CONTRACT_PATH = Path("backend/contracts/mls_column_contract.yaml")
TARGET_TABLE = "stg_mls_classified"


def run_etl(
    xlsx_files: List[str],
    snapshot_date: Optional[date] = None,
    persist: bool = True,
) -> pd.DataFrame:
    """
    Executa o ETL completo:
    - gera import_id único por carga
    - classifica XLSX
    - concatena resultados
    - grava no banco
    """

    snapshot_date = snapshot_date or date.today()
    import_id = uuid4()  # 🔑 UM ID POR CARGA

    dfs = []

    for file_path in xlsx_files:
        df = classify_xlsx(
            xlsx_path=file_path,
            contract_path=CONTRACT_PATH,
            snapshot_date=snapshot_date,
        )

        # 🔗 associa todas as linhas ao mesmo import_id
        df["import_id"] = import_id

        dfs.append(df)

    if not dfs:
        raise ValueError("Nenhum arquivo XLSX processado.")

    final_df = pd.concat(dfs, ignore_index=True)

    # 🔒 validação mínima
    if final_df["import_id"].isna().any():
        raise RuntimeError("import_id não foi corretamente atribuído.")

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
