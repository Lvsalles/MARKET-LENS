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


# -------------------------------------------------
# Logging
# -------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


# -------------------------------------------------
# Configurações
# -------------------------------------------------
CONTRACT_PATH = Path("backend/contracts/mls_column_contract.yaml")
TARGET_TABLE = "stg_mls_classified"


# -------------------------------------------------
# Validação mínima
# -------------------------------------------------
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

    logging.info("Validação do DataFrame: OK")


# -------------------------------------------------
# Persistência
# -------------------------------------------------
def persist_dataframe(df: pd.DataFrame) -> None:
    engine = get_engine()

    logging.info(f"Inserindo {len(df)} registros em {TARGET_TABLE}")

    df.to_sql(
        TARGET_TABLE,
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )

    logging.info("Persistência concluída com sucesso")


# -------------------------------------------------
# Pipeline principal
# -------------------------------------------------
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
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {path}")

        logging.info(f"Processando: {path.name}")

        df = classify_xlsx(
            xlsx_path=path,
            contract_path=CONTRACT_PATH,
            snapshot_date=snapshot_date,
        )

        all_dfs.append(df)

    final_df = pd.concat(all_dfs, ignore_index=True)

    validate_dataframe(final_df)

    if persist:
        persist_dataframe(final_df)

    logging.info("ETL finalizado com sucesso")
    return final_df


# -------------------------------------------------
# Execução direta (cloud job / CI / script)
# -------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Market Lens ETL")
    parser.add_argument(
        "--input",
        nargs="+",
        required=True,
        help="Arquivos XLSX MLS",
    )
    parser.add_argument(
        "--no-db",
        action="store_true",
        help="Não gravar no banco",
    )

    args = parser.parse_args()

    run_etl(
        xlsx_files=args.input,
        persist=not args.no_db,
    )
