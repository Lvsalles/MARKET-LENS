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
# Helpers de Limpeza (Essencial para CSVs do MLS)
# =========================================================

def _clean_numeric(val: Any) -> Any:
    """Remove $, vírgulas e espaços de strings numéricas."""
    if pd.isna(val):
        return None
    if isinstance(val, str):
        # Remove caracteres de moeda e separadores de milhar
        clean_val = val.replace('$', '').replace(',', '').strip()
        try:
            return float(clean_val)
        except:
            return None
    return val

def _safe_json(row: Dict[str, Any]) -> Dict[str, Any]:
    """Prepara o dicionário para virar JSON, tratando datas e NaNs."""
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
# DB Helpers
# =========================================================

def _get_database_url() -> str:
    url = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not url:
        raise RuntimeError("Missing DATABASE_URL or SUPABASE_DB_URL")
    return url

def get_engine() -> Engine:
    return create_engine(_get_database_url(), pool_pre_ping=True)

def _table_columns(engine: Engine, table: str) -> set[str]:
    sql = text("SELECT column_name FROM information_schema.columns WHERE table_name = :t")
    with engine.connect() as conn:
        rows = conn.execute(sql, {"t": table}).fetchall()
    return {r[0] for r in rows}

# =========================================================
# Funções de Inserção
# =========================================================

def _create_import_record(engine: Engine, import_id: str, source_file: str, source_tag: str, snapshot_date: date):
    sql = text("""
        INSERT INTO public.stg_mls_imports (import_id, source_file, source_tag, snapshot_date)
        VALUES (:id, :file, :tag, :dt)
        ON CONFLICT (import_id) DO NOTHING
    """)
    with engine.begin() as conn:
        conn.execute(sql, {"id": import_id, "file": source_file, "tag": source_tag, "dt": snapshot_date})

def _insert_stg_mls_raw(engine: Engine, import_id: str, snapshot_date: date, df_raw: pd.DataFrame) -> int:
    rows = []
    for i, (_, r) in enumerate(df_raw.iterrows(), start=1):
        clean_dict = _safe_json(r.to_dict())
        # Gera hash único da linha para evitar duplicidade
        row_str = json.dumps(clean_dict, sort_keys=True)
        rows.append({
            "import_id": import_id,
            "row_number": i,
            "row_hash": hashlib.sha256(row_str.encode()).hexdigest(),
            "row_json": row_str,
            "snapshot_date": snapshot_date
        })
    
    sql = text("""
        INSERT INTO public.stg_mls_raw (import_id, row_number, row_hash, row_json, snapshot_date)
        VALUES (:import_id, :row_number, :row_hash, :row_json, :snapshot_date)
        ON CONFLICT (row_hash) DO NOTHING
    """)
    with engine.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)

def _insert_stg_mls_classified(engine: Engine, import_id: str, df: pd.DataFrame) -> int:
    # Identifica colunas reais do banco
    db_cols = _table_columns(engine, "stg_mls_classified")
    
    df = df.copy()
    df["import_id"] = import_id
    
    # Filtra colunas que existem no banco
    valid_cols = [c for c in df.columns if c in db_cols]
    df = df[valid_cols]

    # LIMPEZA DE DADOS: Crucial para não dar erro de tipo
    for col in df.columns:
        # Se a coluna for numérica (preço, beds, baths, sqft, etc), limpa caracteres
        if df[col].dtype == 'object':
            # Tenta limpar apenas se parecer numérico ou se for uma coluna conhecida de valores
            if any(word in col.lower() for word in ['price', 'sqft', 'beds', 'baths', 'area', 'tax', 'adom', 'cdom']):
                df[col] = df[col].apply(_clean_numeric)

    # Converte NaNs do Pandas para None do Python (NULL no SQL)
    df = df.replace({np.nan: None})
    
    records = df.to_dict(orient="records")
    if not records:
        return 0

    col_names = ", ".join(df.columns)
    placeholders = ", ".join([f":{c}" for c in df.columns])
    
    sql = text(f"INSERT INTO public.stg_mls_classified ({col_names}) VALUES ({placeholders})")
    
    with engine.begin() as conn:
        conn.execute(sql, records)
    return len(records)

# =========================================================
# EXECUÇÃO PRINCIPAL (Resiliente a CSV e XLSX)
# =========================================================

def run_etl(*, xlsx_file: Any, snapshot_date: date, contract_path: Union[str, Path], source_tag: str = "MLS") -> ETLResult:
    try:
        engine = get_engine()
        import_id = str(uuid.uuid4())
        
        # Detecta extensão
        file_name = xlsx_file.name
        file_ext = Path(file_name).suffix.lower()
        
        # Salva arquivo temporário para leitura
        with tempfile.NamedTemporaryFile(delete=False, suffix=file_ext) as tmp:
            tmp.write(xlsx_file.getbuffer())
            file_path = Path(tmp.name)

        # 1. Cria Registro Mestre
        _create_import_record(engine, import_id, file_name, source_tag, snapshot_date)
        
        # 2. Leitura Automática (CSV ou Excel)
        if file_ext == '.csv':
            df_raw = pd.read_csv(file_path)
        else:
            df_raw = pd.read_excel(file_path, engine="openpyxl")
            
        # 3. Inserção Dados Brutos
        raw_count = _insert_stg_mls_raw(engine, import_id, snapshot_date, df_raw)

        # 4. Classificação (Lógica de IA / Contrato)
        # Nota: Certifique-se que o classify_xlsx trate CSVs também se necessário
        df_classified = classify_xlsx(
            xlsx_path=file_path, 
            contract_path=Path(contract_path), 
            snapshot_date=snapshot_date
        )
        
        # 5. Inserção Dados Classificados
        class_count = _insert_stg_mls_classified(engine, import_id, df_classified)

        # Limpeza do arquivo temporário
        if file_path.exists():
            os.remove(file_path)
            
        return ETLResult(
            ok=True, 
            import_id=import_id, 
            rows_raw_inserted=raw_count, 
            rows_classified_inserted=class_count
        )

    except Exception as e:
        # Se houver erro, tenta deletar o arquivo temporário antes de sair
        if 'file_path' in locals() and file_path.exists():
            os.remove(file_path)
        return ETLResult(ok=False, error=str(e))
