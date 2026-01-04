from __future__ import annotations

import hashlib
import json
import os
import tempfile
import uuid
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from backend.contract.mls_classify import classify_xlsx


# =========================================================
# Result contract
# =========================================================

@dataclass
class ETLResult:
    ok: bool
    import_id: Optional[str] = None
    rows_raw_inserted: int = 0
    rows_classified_inserted: int = 0
    error: Optional[str] = None


# =========================================================
# DB helpers
# =========================================================

def _get_database_url() -> str:
    url = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not url:
        raise RuntimeError("Missing DATABASE_URL or SUPABASE_DB_URL")
    return url


def get_engine() -> Engine:
    return create_engine(_get_database_url(), pool_pre_ping=True)


def _table_columns(engine: Engine, table: str, schema: str = "public") -> set[str]:
    """Detecta as colunas reais da tabela no banco de dados."""
    sql = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema
          AND table_name = :table
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"schema": schema, "table": table}).fetchall()
    return {r[0] for r in rows}


# =========================================================
# Hash / JSON helpers
# =========================================================

def _canonical_json(obj: Dict[str, Any]) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _row_hash(obj: Dict[str, Any]) -> str:
    payload = _canonical_json(obj)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _safe_json(row: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    for k, v in row.items():
        if pd.isna(v):
            out[k] = None
        elif hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


# =========================================================
# CREATE IMPORT RECORD
# =========================================================

def _create_import_record(
    engine: Engine,
    *,
    import_id: str,
    source_file: str,
    source_tag: str,
    snapshot_date: date,
    schema: str = "public"
):
    """Cria o registro pai na tabela stg_mls_imports."""
    cols = _table_columns(engine, "stg_mls_imports", schema)
    data = {
        "import_id": import_id,
        "source_file": source_file,
        "source_tag": source_tag,
        "snapshot_date": snapshot_date
    }
    insert_data = {k: v for k, v in data.items() if k in cols}
    
    if "import_id" not in insert_data:
        return 

    sql = text(f"""
        INSERT INTO {schema}.stg_mls_imports ({", ".join(insert_data.keys())})
        VALUES ({", ".join([f":{k}" for k in insert_data.keys()])})
        ON CONFLICT (import_id) DO NOTHING
    """)
    
    with engine.begin() as conn:
        conn.execute(sql, insert_data)


# =========================================================
# RAW INSERT
# =========================================================

def _insert_stg_mls_raw(
    engine: Engine,
    *,
    import_id: str,
    source_file: str,
    source_tag: str,
    snapshot_date: date,
    df_raw: pd.DataFrame,
    schema: str = "public",
) -> int:

    cols = _table_columns(engine, "stg_mls_raw", schema)
    potential_cols = ["import_id", "source_file", "source_tag", "row_number", "row_hash", "row_json", "snapshot_date"]
    insert_cols = [c for c in potential_cols if c in cols]

    rows: List[Dict[str, Any]] = []

    for i, (_, r) in enumerate(df_raw.iterrows(), start=1):
        clean_dict = _safe_json(r.to_dict())
        rh = _row_hash(clean_dict)

        row_data = {
            "import_id": import_id,
            "row_number": i,
            "row_hash": rh,
            "snapshot_date": snapshot_date
        }

        if "source_file" in cols: row_data["source_file"] = source_file
        if "source_tag" in cols: row_data["source_tag"] = source_tag
        if "row_json" in cols: row_data["row_json"] = json.dumps(clean_dict)

        rows.append({k: row_data[k] for k in insert_cols})

    if not rows:
        return 0

    sql = text(f"""
        INSERT INTO {schema}.stg_mls_raw ({", ".join(insert_cols)})
        VALUES ({", ".join([f":{c}" for c in insert_cols])})
        ON CONFLICT (row_hash) DO NOTHING
    """)

    with engine.begin() as conn:
        conn.execute(sql, rows)

    return len(rows)


# =========================================================
# CLASSIFIED INSERT (TRATAMENTO DE NaN e NULLs)
# =========================================================

def _insert_stg_mls_classified(
    engine: Engine,
    *,
    import_id: str,
    df: pd.DataFrame,
    schema: str = "public",
) -> int:

    cols = _table_columns(engine, "stg_mls_classified", schema)
    
    df = df.copy()
    df["import_id"] = import_id
    
    # Filtra apenas as colunas que existem no banco
    valid_cols = [c for c in df.columns if c in cols]
    df = df[valid_cols]

    # Converte NaN para None (essencial para colunas numéricas do Postgres)
    df = df.replace({np.nan: None})
    records = df.to_dict(orient="records")

    if not records:
        return 0

    sql = text(
        f"INSERT INTO {schema}.stg_mls_classified ({', '.join(df.columns)}) "
        f"VALUES ({', '.join(':'+c for c in df.columns)})"
    )

    with engine.begin() as conn:
        conn.execute(sql, records)

    return len(records)


# =========================================================
# MAIN ETL
# =========================================================

def run_etl(
    *,
    xlsx_file: Any,
    snapshot_date: date,
    contract_path: Union[str, Path],
    source_tag: str = "MLS",
    schema: str = "public",
) -> ETLResult:

    try:
        engine = get_engine()

        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(xlsx_file.getbuffer())
            xlsx_path = Path(tmp.name)

        import_id = str(uuid.uuid4())
        filename = xlsx_path.name

        # 1. Cria o registro PAI
        _create_import_record(
            engine=engine,
            import_id=import_id,
            source_file=filename,
            source_tag=source_tag,
            snapshot_date=snapshot_date,
            schema=schema
        )

        # 2. Lê o arquivo Excel
        df_raw = pd.read_excel(xlsx_path, engine="openpyxl")

        # 3. Insere dados Brutos (RAW)
        raw_count = _insert_stg_mls_raw(
            engine=engine,
            import_id=import_id,
            source_file=filename,
            source_tag=source_tag,
            snapshot_date=snapshot_date,
            df_raw=df_raw,
            schema=schema,
        )

        # 4. Classificação e Processamento
        df_classified = classify_xlsx(
            xlsx_path=xlsx_path,
            contract_path=Path(contract_path),
            snapshot_date=snapshot_date,
        )

        # 5. Insere dados Classificados
        classified_count = _insert_stg_mls_classified(
            engine=engine,
            import_id=import_id,
            df=df_classified,
            schema=schema,
        )

        # Limpeza
        if xlsx_path.exists():
            os.remove(xlsx_path)

        return ETLResult(
            ok=True,
            import_id=import_id,
            rows_raw_inserted=raw_count,
            rows_classified_inserted=classified_count,
        )

    except Exception as e:
        return ETLResult(ok=False, error=str(e))
