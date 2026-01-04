"""
Market Lens — MLS ETL (Cloud-first)

Responsabilidade deste módulo:
- Orquestrar o ETL
- Gerar import_id
- Chamar o classificador determinístico
- Inserir dados no banco

NÃO contém:
- Lógica de classificação
- Regras de negócio MLS
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


# =========================================================
# Helpers
# =========================================================

def _ensure_path(path: str | Path) -> Path:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"File not found: {p}")
    return p


# =========================================================
# Main ETL entrypoint
# =========================================================

def run_etl(
    xlsx_path: str | Path,
    contract_path: str | Path,
    snapshot_date: Optional[date] = None,
) -> dict:
    """
    Executa o ETL completo para um arquivo MLS XLSX.

    Retorna um resumo com:
    - import_id
    - rows_inserted
    """

    snapshot_date = snapshot_date or date.today()

    xlsx_path = _ensure_path(xlsx_path)
    contract_path = _ensure_path(contract_path)

    engine: Engine = get_engine()

    # -----------------------------------------------------
    # 1. Gerar import_id (UUID — FONTE DA VERDADE)
    # -----------------------------------------------------
    import_id = uuid.uuid4()

    # -----------------------------------------------------
    # 2. Classificar XLSX (determinístico)
    # -----------------------------------------------------
    df: pd.DataFrame = classify_xlsx(
        xlsx_path=xlsx_path,
        contract_path=contract_path,
        snapshot_date=snapshot_date,
    )

    if df.empty:
        raise ValueError("Classifier returned empty DataFrame")

    # -----------------------------------------------------
    # 3. Anexar import_id
    # -----------------------------------------------------
    df["import_id"] = import_id

    # -----------------------------------------------------
    # 4. Inserir em stg_mls_classified
    # -----------------------------------------------------
    with engine.begin() as conn:
        df.to_sql(
            name="stg_mls_classified",
            con=conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=500,
        )

    # -----------------------------------------------------
    # 5. Retorno controlado
    # -----------------------------------------------------
    return {
        "import_id": str(import_id),
        "rows_inserted": int(len(df)),
        "snapshot_date": snapshot_date.isoformat(),
        "asset_class": df["asset_class"].iloc[0],
        "executed_at": datetime.utcnow().isoformat(),
    }
