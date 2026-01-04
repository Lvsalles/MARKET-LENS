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
    sql = text("SELECT column_name FROM information_schema.columns WHERE table_schema = :s AND table_name = :t")
    with engine.connect() as conn:
        rows = conn.execute(sql, {"s": schema, "t": table}).fetchall()
    return {r[0] for r in rows}

def _safe_json(row: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    for k, v in row.items():
        if pd.isna(v): out[k] = None
        elif hasattr(v, "isoformat"): out[k] = v.isoformat()
        else: out[k] = v
    return out

def run_etl(*, xlsx_file: Any, snapshot_date: date, contract_path: Union[str, Path], source_tag: str = "MLS") -> ETLResult:
    try:
        engine = get_engine()
        import_id = str(uuid.uuid4())
        
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(xlsx_file.getbuffer())
            xlsx_path = Path(tmp.name)

        # 1. Registro de Importação
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO public.stg_mls_imports (import_id, source_file, source_tag, snapshot_date)
                VALUES (:id, :file, :tag, :dt)
            """), {"id": import_id, "file": xlsx_path.name, "tag": source_tag, "dt": snapshot_date})

        # 2. Leitura e Inserção Raw
        df_raw = pd.read_excel(xlsx_path, engine="openpyxl")
        raw_rows = []
        for i, (_, r) in enumerate(df_raw.iterrows(), start=1):
            clean_dict = _safe_json(r.to_dict())
            raw_rows.append({
                "import_id": import_id,
                "row_number": i,
                "row_hash": hashlib.sha256(json.dumps(clean_dict, sort_keys=True).encode()).hexdigest(),
                "row_json": json.dumps(clean_dict),
                "snapshot_date": snapshot_date
            })
        
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO public.stg_mls_raw (import_id, row_number, row_hash, row_json, snapshot_date)
                VALUES (:import_id, :row_number, :row_hash, :row_json, :snapshot_date)
                ON CONFLICT (row_hash) DO NOTHING
            """), raw_rows)

        # 3. Classificação e Inserção Classified
        df_class = classify_xlsx(xlsx_path=xlsx_path, contract_path=Path(contract_path), snapshot_date=snapshot_date)
        df_class["import_id"] = import_id
        
        # Garante que colunas do DF batam com o banco e trata NaNs
        db_cols = _table_columns(engine, "stg_mls_classified")
        df_class = df_class[[c for c in df_class.columns if c in db_cols]].replace({np.nan: None})
        
        records = df_class.to_dict(orient="records")
        if records:
            col_names = ", ".join(df_class.columns)
            placeholders = ", ".join([f":{c}" for c in df_class.columns])
            with engine.begin() as conn:
                conn.execute(text(f"INSERT INTO public.stg_mls_classified ({col_names}) VALUES ({placeholders})"), records)

        if xlsx_path.exists(): os.remove(xlsx_path)
        return ETLResult(ok=True, import_id=import_id, rows_raw_inserted=len(raw_rows), rows_classified_inserted=len(records))

    except Exception as e:
        return ETLResult(ok=False, error=str(e))
