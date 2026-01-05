import hashlib, json, os, tempfile, uuid
from datetime import date
from pathlib import Path
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

def get_engine():
    url = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    return create_engine(url, pool_pre_ping=True)

def _clean_val(val):
    if pd.isna(val): return None
    if isinstance(val, str):
        v = val.replace('$', '').replace(',', '').strip()
        try: return float(v)
        except: return v
    return val

def run_batch_etl(files_data, report_name, snapshot_date):
    try:
        from backend.contract.mls_classify import classify_xlsx
        engine = get_engine()
        import_id = str(uuid.uuid4())

        with engine.begin() as conn:
            conn.execute(text("INSERT INTO public.stg_mls_imports (import_id, report_name, source_file, source_tag, snapshot_date) VALUES (:id, :n, :f, 'BATCH', :d)"),
                         {"id": import_id, "n": report_name, "f": f"{len(files_data)} files", "d": snapshot_date})

        for item in files_data:
            f, category = item['file'], item['type']
            ext = Path(f.name).suffix.lower()
            with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
                tmp.write(f.getbuffer())
                path = tmp.name

            df_raw = pd.read_csv(path) if ext == '.csv' else pd.read_excel(path)
            # RAW Ingestion
            raw_list = []
            for i, (_, r) in enumerate(df_raw.iterrows(), start=1):
                js = json.dumps({k: (None if pd.isna(v) else v) for k, v in r.to_dict().items()}, default=str)
                raw_list.append({"id": import_id, "n": i, "h": hashlib.sha256(f"{f.name}{js}".encode()).hexdigest(), "j": js, "d": snapshot_date})
            
            with engine.begin() as conn:
                conn.execute(text("INSERT INTO public.stg_mls_raw (import_id, row_number, row_hash, row_json, snapshot_date) VALUES (:id, :n, :h, :j, :d) ON CONFLICT DO NOTHING"), raw_list)

            # CLASSIFIED Ingestion
            df_cls = classify_xlsx(xlsx_path=Path(path), contract_path=Path("backend/contract/mls_column_contract.yaml"), snapshot_date=snapshot_date)
            df_cls["import_id"], df_cls["asset_class"] = import_id, category
            
            for c in df_cls.columns: df_cls[c] = df_cls[c].apply(_clean_val)
            
            records = df_cls.replace({np.nan: None}).to_dict(orient="records")
            if records:
                cols = ", ".join(df_cls.columns)
                vals = ", ".join([f":{c}" for c in df_cls.columns])
                with engine.begin() as conn:
                    conn.execute(text(f"INSERT INTO public.stg_mls_classified ({cols}) VALUES ({vals})"), records)
            os.remove(path)
        return {"ok": True, "import_id": import_id}
    except Exception as e:
        return {"ok": False, "error": str(e)}
