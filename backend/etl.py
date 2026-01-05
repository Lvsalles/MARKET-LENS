from __future__ import annotations
import hashlib, json, os, tempfile, uuid
from datetime import date
from pathlib import Path
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

def get_engine():
    url = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    return create_engine(url, pool_pre_ping=True)

def _clean_numeric(val):
    if pd.isna(val): return None
    if isinstance(val, str):
        v = val.replace('$', '').replace(',', '').strip()
        try: return float(v)
        except: return v
    return val

def run_etl_batch(*, files_data, report_name, snapshot_date, contract_path):
    try:
        from backend.contract.mls_classify import classify_xlsx
        engine = get_engine()
        import_id = str(uuid.uuid4())
        
        with engine.begin() as conn:
            conn.execute(text("INSERT INTO public.stg_mls_imports (import_id, report_name, source_file, source_tag, snapshot_date) VALUES (:id, :name, 'Batch', 'MLS', :d)"),
                         {"id": import_id, "name": report_name, "d": snapshot_date})

        for item in files_data:
            f, f_type = item['file'], item['type']
            ext = Path(f.name).suffix.lower()
            with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
                tmp.write(f.getbuffer())
                path = tmp.name

            df_raw = pd.read_csv(path) if ext == '.csv' else pd.read_excel(path)
            # Process RAW...
            
            df_class = classify_xlsx(xlsx_path=Path(path), contract_path=Path(contract_path), snapshot_date=snapshot_date)
            df_class["import_id"], df_class["asset_class"] = import_id, f_type
            
            # Clean numeric values
            num_cols = ['list_price', 'close_price', 'beds', 'full_baths', 'heated_area', 'tax', 'adom', 'cdom']
            for c in [col for col in num_cols if col in df_class.columns]:
                df_class[c] = df_class[c].apply(_clean_numeric)
            
            records = df_class.replace({np.nan: None}).to_dict(orient="records")
            if records:
                cols = ", ".join(df_class.columns)
                vals = ", ".join([f":{c}" for c in df_class.columns])
                with engine.begin() as conn:
                    conn.execute(text(f"INSERT INTO public.stg_mls_classified ({cols}) VALUES ({vals})"), records)
            os.remove(path)
        return {"ok": True, "import_id": import_id}
    except Exception as e:
        return {"ok": False, "error": str(e)}
