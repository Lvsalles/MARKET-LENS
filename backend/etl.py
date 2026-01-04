"""
Market Lens — MLS ETL (Cloud-first, schema-aligned, FK-safe)

Este ETL respeita EXATAMENTE o schema atual:
stg_mls_raw:
- import_id (UUID)
- snapshot_date (DATE)
- imported_at (TIMESTAMP)
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from uuid import uuid4
import tempfile

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
    if isinstance(xlsx_input, (str, Path)):
        return Path(xlsx_input)

    if hasattr(xlsx_input, "getbuffer") and hasattr(xlsx_input, "name"):
        suffix = Path(xlsx_input.name).suffix or ".xlsx"
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        with tmp as f:
            f.write(xlsx_input.getbuffer())
        return Path(tmp.name)

    raise TypeError(f"xlsx_input type unsupported: {type(xlsx_input)}")


def _normalize_contract_path(contract_path) -> Path:
    if isinstance(contract_path, (str, Path)):
        return Path(contract_path)
    raise TypeError(f"contract_path must be str|Path, got {type(contract_path)}")


# =========================================================
# RAW insert (ALINHADO AO SCHEMA REAL)
# =========================================================

def insert_raw(engine, *, import_id, snapshot_date: date):
    """
    Insere APENAS colunas que existem em stg_mls_raw
    """
    sql = text(
        """
        INSERT INTO stg_mls_raw (
            import_id,
            snapshot_date,
            imported_at
        )
        VALUES (
            :import_id,
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
                "snapshot_date": snapshot_date,
            },
        )


# =========================================================
# MAIN ETL (DEFINITIVO)
# =========================================================

def run_etl(
    *,
    xlsx_path,
    contract_path,
    snapshot_date: date | None = None,
):
    """
    ETL MLS resiliente, alinhado ao schema real.
    """

    snapshot_date = snapshot_date or date.today()

    # Normalização de inputs
    xlsx_path = _normalize_xlsx_input(xlsx_path)
    contract_path = _normalize_contract_path(contract_path)

    engine = get_engine()
    import_id = uuid4()

    # 1) RAW (tabela pai)
    insert_raw(
        engine,
        import_id=import_id,
        snapshot_date=snapshot_date,
    )

    # 2) CLASSIFY
    df = classify_xlsx(
        xlsx_path=xlsx_path,
        contract_path=contract_path,
        snapshot_date=snapshot_date,
    )

    if df is None or df.empty:
        raise ValueError("Classificação retornou DataFrame vazio")

    df["import_id"] = str(import_id)

    # 3) CLASSIFIED (tabela filha)
    df.to_sql(
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
        "rows_inserted": int(len(df)),
        "snapshot_date": snapshot_date.isoformat(),
    }
