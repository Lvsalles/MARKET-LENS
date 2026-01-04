from __future__ import annotations

import uuid
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from backend.db import get_engine
from backend.contract.mls_classify import classify_xlsx


def run_etl(
    xlsx_path: str | Path,
    *,
    contract_path: str | Path,
    source_tag: str = "MLS",
    engine: Optional[Engine] = None,
) -> dict:
    """
    ETL definitivo para Market Lens.

    stg_mls_raw (contrato REAL):
      - import_id   (uuid, not null)
      - source_file (text, not null)
      - source_tag  (text, not null)
      - row_number  (int, not null)

    Nenhuma outra coluna é assumida.
    """

    engine = engine or get_engine()

    xlsx_path = Path(xlsx_path)
    contract_path = Path(contract_path)

    if not xlsx_path.exists():
        raise FileNotFoundError(f"XLSX não encontrado: {xlsx_path}")

    if not contract_path.exists():
        raise FileNotFoundError(f"Contract YAML não encontrado: {contract_path}")

    # --------------------------------------------------
    # 1) Gerar import_id
    # --------------------------------------------------
    import_id = str(uuid.uuid4())

    # --------------------------------------------------
    # 2) Ler XLSX bruto
    # --------------------------------------------------
    raw_df = pd.read_excel(xlsx_path, engine="openpyxl")

    if raw_df.empty:
        raise ValueError("Arquivo XLSX está vazio")

    # --------------------------------------------------
    # 3) Inserir RAW (1 linha por row do XLSX)
    # --------------------------------------------------
    raw_rows = [
        {
            "import_id": import_id,
            "source_file": xlsx_path.name,
            "source_tag": source_tag,
            "row_number": idx + 1,
        }
        for idx in range(len(raw_df))
    ]

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO public.stg_mls_raw
                (import_id, source_file, source_tag, row_number)
                VALUES
                (:import_id, :source_file, :source_tag, :row_number)
                """
            ),
            raw_rows,
        )

    # --------------------------------------------------
    # 4) Classificação MLS
    # --------------------------------------------------
    classified_df = classify_xlsx(
        xlsx_path=xlsx_path,
        contract_path=contract_path,
        snapshot_date=date.today(),
    )

    classified_df["import_id"] = import_id

    # --------------------------------------------------
    # 5) Inserir classificados
    # --------------------------------------------------
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
        "raw_rows": len(raw_rows),
        "classified_rows": len(classified_df),
    }
