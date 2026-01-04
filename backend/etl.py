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

@dataclass
class ETLResult:
    ok: bool
    import_id: Optional[str] = None
    rows_raw_inserted: int = 0
    rows_classified_inserted: int = 0
    error: Optional[str] = None

def _get_database_url() -> str:
    url = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not url:
        raise RuntimeError("Missing DATABASE_URL or SUPABASE_DB_URL")
    return url

def get_engine() -> Engine:
    return create_engine(_get_database_url(), pool_pre_ping=True)

def _table_columns(engine: Engine, table: str, schema: str = "public") -> set[str]:
    sql = text("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = :schema AND table_name = :table
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"schema": schema, "table": table}).fetchall()
    return {r[0] for r in rows}

def _row_hash(obj: Dict[str, Any]) -> str:
    payload = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

def _safe_json(row: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    for k, v in row.items():
        if pd.isna(v): out[k] = None
        elif hasattr(v, "isoformat"): out[k] = v.isoformat()
        else: out[k] = v
    return out

# 1. CRIA O REGISTRO MESTRE
def _create_import_record(engine: Engine, import_id: str, source_file: str, source_tag: str, snapshot_date: date):
    sql = text("""
        INSERT INTO public.stg_mls_imports (import_id, source_file, source_tag, snapshot_date)
        VALUES (:import_id, :source_file, :source_tag, :snapshot_date)
    """)
    with engine.begin() as conn:
        conn.execute(sql, {"import_id": import_id, "source_file": source_file, "source_tag": source_tag, "snapshot_date": snapshot_date})

# 2. INSERE DADOS BRUTOS
def _insert_stg_mls_raw(engine: Engine, import_id: str, snapshot_date: date, df_raw: pd.DataFrame) -> int:
    rows = []
    for i, (_, r) in enumerate(df_raw.iterrows(), start=1):
        clean_dict = _safe_json(r.to_dict())
        rows.append({
            "import_id": import_id,
            "row_number": i,
            "row_hash": _row_hash(clean_dict),
            "row_json": json.dumps(clean_dict),
            "snapshot_date": snapshot_date
        })
    
    sql = text("""
        INSERT INTO public.stg_mls_raw (import_id, row_number, row_hash, row_json, snapshot_date)
        VALUES (:import_id, :row_number, :row_hash, :row_json, :snapshot_date)
        ON CONFLICT (import_id, row_number) DO NOTHING
    """)
    with engine.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)

# 3. INSERE DADOS PROCESSADOS
def _insert_stg_mls_classified(engine: Engine, import_id: str, df: pd.DataFrame) -> int:
    cols = _table_columns(engine, "stg_mls_classified")
    df = df.copy()
    df["import_id"] = import_id
    valid_cols = [c for c in df.columns if c in cols]
    df = df[valid_cols].replace({np.nan: None})
    
    records = df.to_dict(orient="records")
    if not records: return 0

    sql = text(f"INSERT INTO public.stg_mls_classified ({', '.join(df.columns)}) VALUES ({', '.join(':'+c for c in df.columns)})")
    with engine.begin() as conn:
        conn.execute(sql, records)
    return len(records)

# FUNÇÃO PRINCIPAL
def run_etl(*, xlsx_file: Any, snapshot_date: date, contract_path: Union[str, Path], source_tag: str = "MLS") -> ETLResult:
    try:
        engine = get_engine()
        import_id = str(uuid.uuid4())
        
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(xlsx_file.getbuffer())
            xlsx_path = Path(tmp.name)

        # Passo 1: Registro Mestre
        _create_import_record(engine, import_id, xlsx_path.name, source_tag, snapshot_date)
        
        # Passo 2: Dados Brutos
        df_raw = pd.read_excel(xlsx_path, engine="openpyxl")
        raw_count = _insert_stg_mls_raw(engine, import_id, snapshot_date, df_raw)

        # Passo 3: Classificação via IA
        df_classified = classify_xlsx(xlsx_path=xlsx_path, contract_path=Path(contract_path), snapshot_date=snapshot_date)
        
        # Passo 4: Dados Classificados
        class_count = _insert_stg_mls_classified(engine, import_id, df_classified)

        os.remove(xlsx_path)
        return ETLResult(ok=True, import_id=import_id, rows_raw_inserted=raw_count, rows_classified_inserted=class_count)
    except Exception as e:
        return ETLResult(ok=False, error=str(e))
