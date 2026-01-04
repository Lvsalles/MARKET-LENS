"""
Market Lens — MLS ETL (Cloud-first, UI-safe, FK-safe)

- Aceita xlsx como Path/str ou Streamlit UploadedFile
- Exige contract_path como Path/str (keyword-only)
- snapshot_date opcional
- Valida tipos com mensagens claras
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

    # Streamlit UploadedFile
    if hasattr(xlsx_input, "getbuffer") and hasattr(xlsx_input, "name"):
        suffix = Path(xlsx_input.name).suffix or ".xlsx"
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        with tmp as f:
            f.write(xlsx_input.getbuffer())
        return Path(tmp.name)

    raise TypeError(f"xlsx_input type unsupported: {type(xlsx_input)}")


def _normalize_contract_path(contract_path) -> Path:
    """
    contract_path DEVE ser str|Path.
    Se vier date, é 100% indicação de argumentos trocados na chamada.
    """
    if isinstance(contract_path, (str, Path)):
        return Path(contract_path)

    if isinstance(contract_path, date):
        raise TypeError(
            "contract_path recebeu um 'date'. Isso indica que a chamada do run_etl() "
            "está com argumentos trocados. Chame assim:\n"
            "run_etl(xlsx_path=uploaded_file, contract_path='.../mls_contract.yaml', snapshot_date=...)"
        )

    raise TypeError(f"contract_path type unsupported: {type(contract_path)}")


def insert_raw(engine, *, import_id, filename: str, snapshot_date: date):
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
            {"import_id": str(import_id), "filename": filename, "snapshot_date": snapshot_date},
        )


# =========================================================
# MAIN ETL (KEYWORD-ONLY)
# =========================================================

def run_etl(
    *,
    xlsx_path,
    contract_path,
    snapshot_date: date | None = None,
):
    """
    IMPORTANTÍSSIMO:
    - keyword-only para impedir erro de ordem (positional)
    """

    snapshot_date = snapshot_date or date.today()

    # validações fortes (mensagens claras)
    if not isinstance(snapshot_date, date):
        raise TypeError(f"snapshot_date deve ser datetime.date, recebeu: {type(snapshot_date)}")

    xlsx_path = _normalize_xlsx_input(xlsx_path)
    contract_path = _normalize_contract_path(contract_path)

    engine = get_engine()
    import_id = uuid4()

    # 1) RAW (pai)
    insert_raw(engine, import_id=import_id, filename=xlsx_path.name, snapshot_date=snapshot_date)

    # 2) CLASSIFY
    df = classify_xlsx(
        xlsx_path=xlsx_path,
        contract_path=contract_path,
        snapshot_date=snapshot_date,
    )

    if df is None or df.empty:
        raise ValueError("Classificação retornou DataFrame vazio. Verifique o XLSX e o contrato.")

    df["import_id"] = str(import_id)

    # 3) CLASSIFIED (filho)
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
        "filename": xlsx_path.name,
    }
