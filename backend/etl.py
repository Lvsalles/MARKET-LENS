"""
Market Lens — MLS ETL (Cloud-first)

Responsabilidade:
- Gerar import_id (UUID)
- Inserir o "pai" em stg_mls_imports
- Classificar XLSX
- Inserir em stg_mls_classified

NÃO contém:
- regras de classificação (isso fica no contract/classifier)
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Optional
import uuid

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from backend.db import get_engine
from backend.contracts.mls_classify import classify_xlsx


def _ensure_path(path: str | Path) -> Path:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"File not found: {p}")
    return p


def run_etl(
    xlsx_path: str | Path,
    contract_path: str | Path,
    snapshot_date: Optional[date] = None,
) -> dict:
    snapshot_date = snapshot_date or date.today()

    xlsx_path = _ensure_path(xlsx_path)
    contract_path = _ensure_path(contract_path)

    engine: Engine = get_engine()

    import_id = uuid.uuid4()
    source_filename = xlsx_path.name

    # 1) Classificar XLSX
    df: pd.DataFrame = classify_xlsx(
        xlsx_path=xlsx_path,
        contract_path=contract_path,
        snapshot_date=snapshot_date,
    )

    if df.empty:
        raise ValueError("Classifier returned empty DataFrame")

    # 2) Anexar import_id
    df["import_id"] = import_id

    # 3) Transação: insere pai + insere dados
    with engine.begin() as conn:
        # 3.1) Inserir pai na tabela de imports
        conn.execute(
            text("""
                insert into stg_mls_imports (import_id, snapshot_date, source_filename)
                values (:import_id, :snapshot_date, :source_filename)
            """),
            {
                "import_id": str(import_id),
                "snapshot_date": snapshot_date,
                "source_filename": source_filename,
            },
        )

        # 3.2) Inserir stg_mls_classified
        df.to_sql(
            name="stg_mls_classified",
            con=conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=500,
        )

    return {
        "import_id": str(import_id),
        "rows_inserted": int(len(df)),
        "snapshot_date": snapshot_date.isoformat(),
        "asset_class": df["asset_class"].iloc[0],
        "source_filename": source_filename,
        "executed_at": datetime.utcnow().isoformat(),
    }
