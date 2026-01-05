from __future__ import annotations
import hashlib, json, os, tempfile, uuid
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

@dataclass
class ETLResult:
    ok: bool
    import_id: Optional[str] = None
    error: Optional[str] = None

def get_engine():
    """Main database engine provider."""
    url = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not url:
        raise RuntimeError("Database URL environment variable is missing.")
    return create_engine(url, pool_pre_ping=True)

def _clean_numeric(val: Any) -> Any:
    """Cleans currency strings like '$1,234.50' into floats."""
    if pd.isna(val): return None
    if isinstance(val, str):
        clean_val = val.replace('$', '').replace(',', '').strip()
        try: return float(clean_val)
        except: return None
    return val

def run_etl_batch(*, files_data: List[Dict[str, Any]], report_name: str, snapshot_date: date, contract_path: str) -> ETLResult:
    """Processes up to 25 files into a single isolated report."""
    try:
        from backend.contract.mls_classify import classify_xlsx
        engine = get_engine()
        import_id = str(uuid.uuid4())
        
        # 1. Create Master Record for this specific report
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO public.stg_mls_imports (import_id, report_name, source_file, source_tag, snapshot_date) 
                VALUES (:id, :name, :f, 'BATCH', :d)
            """), {"id": import_id, "name": report_name, "f": f"{len(files_data)} files", "d": snapshot_date})

        for item in files_data:
            uploaded_file = item['file']
            data_type = item['type']
            file_ext = Path(uploaded_file.name).suffix.lower()

            with tempfile.NamedTemporaryFile(delete=False, suffix=file_ext) as tmp:
                tmp.write(uploaded_file.getbuffer())
                file_path = Path(tmp.name)

            # 2. Process Raw Data
            df_raw = pd.read_csv(file_path) if file_ext == '.csv' else pd.read_excel(file_path)
            raw_rows = []
            for i, (_, r) in enumerate(df_raw.iterrows(), start=1):
                clean_dict = {k: (None if pd.isna(v) else v) for k, v in r.to_dict().items()}
                js = json.dumps(clean_dict, default=str)
                raw_rows.append({
                    "id": import_id, "n": i, 
                    "h": hashlib.sha256(f"{uploaded_file.name}{js}".encode()).hexdigest(), 
                    "j": js, "d": snapshot_date
                })
            
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO public.stg_mls_raw (import_id, row_number, row_hash, row_json, snapshot_date) 
                    VALUES (:id, :n, :h, :j, :d) ON CONFLICT DO NOTHING
                """), raw_rows)

            # 3. Process Classified Data
            df_class = classify_xlsx(xlsx_path=file_path, contract_path=Path(contract_path), snapshot_date=snapshot_date)
            df_class["import_id"] = import_id
            df_class["asset_class"] = data_type # User-defined classification
            
            # Clean numeric columns to avoid "out of range" or type errors
            num_cols = ['list_price', 'close_price', 'beds', 'full_baths', 'heated_area', 'tax', 'adom', 'cdom']
            for c in [col for col in num_cols if col in df_class.columns]:
                df_class[c] = df_class[c].apply(_clean_numeric)
            
            df_class = df_class.replace({np.nan: None})
            records = df_class.to_dict(orient="records")
            
            if records:
                cols_list = ", ".join(df_class.columns)
                vals_list = ", ".join([f":{c}" for c in df_class.columns])
                with engine.begin() as conn:
                    conn.execute(text(f"INSERT INTO public.stg_mls_classified ({cols_list}) VALUES ({vals_list})"), records)

            os.remove(file_path)

        return ETLResult(ok=True, import_id=import_id)
    except Exception as e:
        return ETLResult(ok=False, error=str(e))
