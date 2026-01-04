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
# RAW INSERT (CORRIGIDO: Alterado 'src' para 'row_json')
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

    # Verificação básica de colunas obrigatórias
    required = {"import_id", "row_number", "row_hash"}
    missing = required - cols
    if missing:
        raise RuntimeError(f"stg_mls_raw missing required columns: {missing}")

    # Definimos quais colunas vamos tentar inserir. 
    # IMPORTANTE: Mudamos 'src' para 'row_json' para bater com o Banco de Dados.
    insert_cols = [
        c for c in [
            "import_id",
            "source_file",
            "source_tag",
            "row_number",
            "row_hash",
            "row_json",  
        ]
        if c in cols
    ]

    rows: List[Dict[str, Any]] = []

    for i, (_, r) in enumerate(df_raw.iterrows(), start=1):
        # Transforma a linha do Excel em um dicionário limpo
        clean_row_dict = _safe_json(r.to_dict())
        
        # Gera o hash único da linha para evitar duplicatas
        rh = _row_hash(clean_row_dict)

        # Prepara o dicionário de dados para o INSERT
        row_data = {
            "import_id": import_id,
            "row_number": i,
            "row_hash": rh,
        }

        if "source_file" in cols:
            row_data["source_file"] = source_file
        if "source_tag" in cols:
            row_data["source_tag"] = source_tag
        
        # Aqui é onde o erro acontecia. Agora populamos 'row_json' corretamente.
        if "row_json" in cols:
            row_data["row_json"] = json.dumps(clean_row_dict)

        # Adiciona apenas as colunas que existem de fato na tabela
        rows.append({k: row_data[k] for k in insert_cols})

    if not rows:
        return 0

    col_list = ", ".join(insert_cols)
    val_list = ", ".join([f":{c}" for c in insert_cols])

    # SQL dinâmico baseado nas colunas detectadas
    sql = text(f"""
        INSERT INTO {schema}.stg_mls_raw ({col_list})
        VALUES ({val_list})
        ON CONFLICT (row_hash) DO NOTHING
    """)

    with engine.begin() as conn:
        conn.execute(sql, rows)

    return len(rows)


# =========================================================
# CLASSIFIED INSERT
# =========================================================

def _insert_stg_mls_classified(
    engine: Engine,
    *,
    import_id: str,
    df: pd.DataFrame,
    schema: str = "public",
) -> int:

    cols = _table_columns(engine, "stg_mls_classified", schema)
    if "import_id" not in cols:
        raise RuntimeError("stg_mls_classified missing import_id")

    df = df.copy()
    df["import_id"] = import_id
    df = df[[c for c in df.columns if c in cols]]
    df = df.where(pd.notnull(df), None)

    if df.empty:
        return 0

    sql = text(
        f"INSERT INTO {schema}.stg_mls_classified "
        f"({', '.join(df.columns)}) "
        f"VALUES ({', '.join(':'+c for c in df.columns)})"
    )

    with engine.begin() as conn:
        conn.execute(sql, df.to_dict(orient="records"))

    return len(df)


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

        # Cria arquivo temporário para leitura do Excel
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(xlsx_file.getbuffer())
            xlsx_path = Path(tmp.name)

        import_id = str(uuid.uuid4())

        # Lê o arquivo Excel
        df_raw = pd.read_excel(xlsx_path, engine="openpyxl")

        # 1. Insere os dados brutos (Onde corrigimos o erro)
        raw_count = _insert_stg_mls_raw(
            engine=engine,
            import_id=import_id,
            source_file=xlsx_path.name,
            source_tag=source_tag,
            snapshot_date=snapshot_date,
            df_raw=df_raw,
            schema=schema,
        )

        # 2. Classifica os dados usando a IA/Contrato
        df_classified = classify_xlsx(
            xlsx_path=xlsx_path,
            contract_path=Path(contract_path),
            snapshot_date=snapshot_date,
        )

        # 3. Insere os dados classificados para análise da Webtool
        classified_count = _insert_stg_mls_classified(
            engine=engine,
            import_id=import_id,
            df=df_classified,
            schema=schema,
        )

        # Remove o arquivo temporário após o sucesso
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
