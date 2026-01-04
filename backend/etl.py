"""
Market Lens — MLS ETL (Cloud-first, FK-safe)

Pipeline:
1) Gera import_id (UUID)
2) Insere RAW (stg_mls_raw)
3) Classifica XLSX
4) Insere CLASSIFIED (stg_mls_classified) usando o mesmo import_id
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from uuid import uuid4

import pandas as pd
from sqlalchemy import text

from backend.db import get_engine
from backend.core.mls_classify import classify_xlsx



# =========================================================
# Helpers
# =========================================================

def insert_raw(
    engine,
    *,
    import_id,
    filename: str,
    snapshot_date: date,
):
    """
    Insere uma linha em stg_mls_raw (tabela pai)
    """
    sql = text(
        """
        INSERT INTO stg_mls_raw (
            import_id,
            filename,
            snapshot_date,
            imported_at
        )
        VALUES (
            :import_id,
            :filename,
            :snapshot_date,
            NOW()
        )
        """
    )

    with engine.begin() as conn:
        conn.execute(
            sql,
            {
                "import_id": str(import_id),
                "filename": filename,
                "snapshot_date": snapshot_date,
            },
        )


# =========================================================
# Main ETL
# =========================================================

def run_etl(
    *,
    xlsx_path: str | Path,
    contract_path: str | Path,
    snapshot_date: date,
):
    """
    Executa o ETL completo para um arquivo MLS
    """

    engine = get_engine()

    # 1. Gerar import_id único
    import_id = uuid4()

    xlsx_path = Path(xlsx_path)
    contract_path = Path(contract_path)

    # 2. Inserir RAW (PAI)
    insert_raw(
        engine,
        import_id=import_id,
        filename=xlsx_path.name,
        snapshot_date=snapshot_date,
    )

    # 3. Classificar XLSX
    classified_df = classify_xlsx(
        xlsx_path=xlsx_path,
        contract_path=contract_path,
        snapshot_date=snapshot_date,
    )

    if classified_df.empty:
        raise ValueError("Classified DataFrame is empty")

    # 4. Anexar import_id (UUID)
    classified_df["import_id"] = str(import_id)

    # 5. Inserir CLASSIFIED (FILHO)
    classified_df.to_sql(
        "stg_mls_classified",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500,
    )

    return {
        "import_id": str(import_id),
        "rows_inserted": len(classified_df),
    }
