"""
ETL Orchestrator — Market Lens

Responsabilidade:
- Orquestrar o pipeline de ingestão
- Chamar o classificador determinístico (mls_classify.py)
- Validar a saída
- Persistir no banco

⚠️ NÃO contém regras de negócio
⚠️ NÃO classifica ativo
⚠️ NÃO interpreta colunas
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import List, Optional

import pandas as pd

from backend.etl.mls_classify import classify_file, load_contract
from backend.db.db import get_engine  # ajuste se seu import for diferente


# -------------------------------------------------
# Configuração básica de logging
# -------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


# -------------------------------------------------
# Configuração central do ETL
# -------------------------------------------------
CONTRACT_PATH = Path("backend/contracts/mls_column_contract.yaml")


# -------------------------------------------------
# Validação mínima do DataFrame classificado
# -------------------------------------------------
REQUIRED_COLUMNS = {
    "asset_class",
    "status_group",
    "ml_number",
    "list_price",
    "close_price",
    "snapshot_date",
}


def validate_classified_dataframe(df: pd.DataFrame) -> None:
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(
            f"Classified DataFrame inválido. Colunas faltando: {missing}"
        )

    if df["ml_number"].isna().any():
        raise ValueError("Existem linhas sem ML Number — ingestão abortada.")

    logging.info("Validação do DataFrame classificado: OK")


# -------------------------------------------------
# Persistência no banco
# -------------------------------------------------
def persist_to_database(
    df: pd.DataFrame,
    table_name: str = "stg_mls_classified",
    if_exists: str = "append",
) -> None:
    engine = get_engine()

    logging.info(f"Inserindo {len(df)} registros na tabela {table_name}")

    df.to_sql(
        table_name,
        engine,
        if_exists=if_exists,
        index=False,
        method="multi",
        chunksize=1000,
    )

    logging.info("Persistência concluída com sucesso")


# -------------------------------------------------
# Pipeline principal
# -------------------------------------------------
def run_etl(
    input_files: List[str],
    snapshot_date: Optional[date] = None,
    persist: bool = True,
) -> pd.DataFrame:
    """
    Executa o pipeline completo de ETL.

    Retorna o DataFrame classificado final.
    """

    logging.info("Iniciando ETL Market Lens")

    if not CONTRACT_PATH.exists():
        raise FileNotFoundError(
            f"Contrato não encontrado: {CONTRACT_PATH}"
        )

    contract = load_contract(CONTRACT_PATH)

    snap_date = snapshot_date or date.today()

    classified_dfs = []

    for file_path in input_files:
        path = Path(file_path)

        if not path.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {path}")

        logging.info(f"Processando arquivo: {path.name}")

        df_classified, _ = classify_file(
            xlsx_path=path,
            contract=contract,
            snapshot_date=snap_date,
        )

        classified_dfs.append(df_classified)

    final_df = pd.concat(classified_dfs, ignore_index=True)

    validate_classified_dataframe(final_df)

    if persist:
        persist_to_database(final_df)

    logging.info("ETL finalizado com sucesso")

    return final_df


# -------------------------------------------------
# Execução direta (CLI)
# -------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Market Lens ETL Orchestrator")
    parser.add_argument(
        "--input",
        nargs="+",
        required=True,
        help="Lista de arquivos XLSX MLS",
    )
    parser.add_argument(
        "--no-db",
        action="store_true",
        help="Não persistir no banco (apenas classificar)",
    )

    args = parser.parse_args()

    run_etl(
        input_files=args.input,
        persist=not args.no_db,
    )
