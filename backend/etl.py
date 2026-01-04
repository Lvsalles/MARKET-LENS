"""
Market Lens — MLS ETL (FINAL, schema-proof)

Este ETL:
- Respeita EXATAMENTE o schema atual do banco
- stg_mls_raw: apenas (import_id, imported_at)
- stg_mls_classified: recebe os dados classificados + import_id
- Funciona com Streamlit UploadedFile
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
# RAW insert (100% alinhado ao schema REAL)
# =========================================================

def insert_raw(engine, *, import_id):
    """
    Insere APENAS o que existe em stg_mls_raw
    """
    sql = text(
        """
        INSERT INTO stg_mls_raw (
            import_id,
            imported_at
        )
        VALUES (
            :import_id,
            NOW()
        )
        """
    )

    with engine.begin() as conn:
        conn.execute(
            sql,
            {"import_id": str(import_id)},
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
    ETL MLS FINAL:
    - Não assume colunas inexistentes
    - snapshot_date é usado apenas para classificação (não RAW)
    """

    snapshot_date = snapshot_date or date.today()

    # Normalização de inputs
    xlsx_path = _normalize_xlsx_input(xlsx_path)
    contract_path = _normalize_contract_path(contract_path)

    engine = get_engine()
    import_id = uuid4()

    # 1) RAW (tabela pai)
