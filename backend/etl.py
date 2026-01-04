"""
Market Lens — MLS ETL (Cloud-first, UI-safe, FK-safe)

Aceita:
- path (str | Path)
- UploadedFile (Streamlit)

Normaliza tudo internamente.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from uuid import uuid4
import tempfile
import os

import pandas as pd
from sqlalchemy import text

from backend.db import get_engine
from backend.core.mls_classify import classify_xlsx


# =========================================================
# Helpers
# =========================================================

def _normalize_xlsx_input(xlsx_input) -> Path:
    """
    Aceita Path | str | Streamlit UploadedFile
    Retorna Path válido no filesystem
    """

    # Já é path
    if isinstance(xlsx_input, (str, Path)):
        return Path(xlsx_input)

    # UploadedFile (Streamlit)
    if hasattr(xlsx_input, "getbuffer") and hasattr(xlsx_input, "name"):
        suffix = Path(xlsx_input.name).suffix or ".xlsx"

        tmp = tempfile.NamedTemporaryFile(
            delete=False,
            suffix=suffix,
        )

        with tmp as f:
            f.write(xlsx_input.getbuffer())

        return Path(tmp.name)

    raise TypeError(
        f"Unsupported xlsx_input type: {type(xlsx_input)}"
    )


def insert_raw(
    engine,
    *,
    import_id,
    filename: str,
    snapshot_date: date,
):
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
# MAIN ETL (RESILIENTE)
# =========================================================

def run_etl(
    xlsx_path,
    contract_path: str | Path,
    snapshot_date: date | None = None,
):
    """
    ETL MLS resiliente:
    - aceita UploadedFile ou Path
    - snapshot_date opcional
    """

    snapshot_date = snapshot_date or date.today()

    engine = get_engine()
    import_id = uuid4()

    # --- normalização crítica ---
    xlsx_path = _normalize_xlsx_input(xlsx_path)
    contract_path = Path(contract_path)

    # 1. RAW (pai)
    insert_raw(
        engine,
        import_id=import_id,
        filename=xlsx_path.name,
        snapshot_date=snapshot_date,
    )

    # 2. CLASSIFY
    classified_df = classify_xlsx(
        xlsx_path=xlsx_path,
        contract_path=contract_path,
        snapshot_date=snapshot_date,
    )

    if classified_df.empty:
        raise ValueError("Classified DataFrame is empty")

    classified_df["import_id"] = str(import_id)

    # 3. CLASSIFIED (filho)
    classified_df.to_sql(
        "stg_mls_classified",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500,
    )

    return {
        "status": "success",
        "import_id": str(import_id),
        "rows_inserted": len(classified_df),
        "snapshot_date": snapshot_date.isoformat(),
    }
