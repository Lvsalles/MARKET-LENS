from __future__ import annotations
import hashlib, json, os, tempfile, uuid
from datetime import date
from pathlib import Path
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

def get_engine():
    url = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return create_engine(url, pool_pre_ping=True)

def _clean_numeric(val):
    if pd.isna(val): return None
    if isinstance(val, str):
        v = val.replace('$', '').replace(',', '').strip()
        try: return float(v)
        except: return None
    return val

def run_batch_etl(files_data, report_name, snapshot_date):
    try:
        from backend.contract.mls_classify import classify_xlsx
        engine = get_engine()
        import_id = str(uuid.uuid4())
        
        # 1. Create Master Silo
        with engine.begin() as conn:
            conn.execute(text("INSERT INTO public.stg_mls_imports (import_id, report_name, source_file, source_tag, snapshot_date) VALUES (:id, :n, 'Batch', 'MLS', :d)"),
                         {"id": import_id, "n": report_name, "d": snapshot_date})

        for item in files_data:
            f, category = item['file'], item['type']
            ext = Path(f.name).suffix.lower()
            with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
                tmp.write(f.getbuffer())
                path = tmp.name

            # 2. Classify and force category
            df_cls = classify_xlsx(xlsx_path=Path(path), contract_path=Path("backend/contract/mls_column_contract.yaml"), snapshot_date=snapshot_date)
            df_cls["import_id"] = import_id
            df_cls["asset_class"] = category # Forces 'Properties', 'Land', or 'Rental'

            # Clean numbers
            num_cols = ['list_price', 'close_price', 'beds', 'full_baths', 'heated_area', 'tax', 'adom', 'cdom']
            for c in [col for col in num_cols if col in df_cls.columns]:
                df_cls[c] = df_cls[c].apply(_clean_numeric)

            records = df_cls.replace({np.nan: None}).to_dict(orient="records")
            if records:
                cols, vals = ", ".join(df_cls.columns), ", ".join([f":{c}" for c in df_cls.columns])
                with engine.begin() as conn:
                    conn.execute(text(f"INSERT INTO public.stg_mls_classified ({cols}) VALUES ({vals})"), records)
            os.remove(path)
        return {"ok": True, "import_id": import_id}
    except Exception as e:
        return {"ok": False, "error": str(e)}
