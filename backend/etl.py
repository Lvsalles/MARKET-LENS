from __future__ import annotations

import json
import os
import tempfile
import uuid
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ✅ estrutura REAL do seu repo (singular: contract)
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
    # Streamlit Cloud geralmente usa secrets/env
    url = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not url:
        raise RuntimeError("Missing env var DATABASE_URL (or SUPABASE_DB_URL)")
    return url


def get_engine() -> Engine:
    return create_engine(_get_database_url(), pool_pre_ping=True)


def _table_columns(engine: Engine, table: str, schema: str = "public") -> Set[str]:
    sql = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema
          AND table_name = :table
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"schema": schema, "table": table}).fetchall()
    return {r[0] for r in rows}


def _safe_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Garante que o 'src' seja um JSON OBJECT válido (dict),
    convertendo tipos não serializáveis.
    """
    out: Dict[str, Any] = {}
    for k, v in row.items():
        if isinstance(v, (pd.Timestamp,)):
            out[k] = v.isoformat()
        elif hasattr(v, "isoformat") and callable(getattr(v, "isoformat")):
            # date/datetime
            out[k] = v.isoformat()
        elif pd.isna(v):
            out[k] = None
        else:
            out[k] = v
    return out


# =========================================================
# Core inserts (resiliente ao schema)
# =========================================================

def _insert_stg_mls_raw(
    engine: Engine,
    import_id: str,
    source_file: str,
    source_tag: str,
    snapshot_date: date,
    df_raw: pd.DataFrame,
    schema: str = "public",
) -> int:
    """
    Insere linhas RAW de forma resiliente:
    - Descobre quais colunas existem em stg_mls_raw
    - Preenche as colunas usuais (import_id, source_file, source_tag, row_number, src)
    - NÃO tenta inserir colunas que não existem (ex: snapshot_date / imported_at etc)
    """
    cols = _table_columns(engine, "stg_mls_raw", schema=schema)

    # colunas candidatas (vamos usar só as que existirem)
    wanted = ["import_id", "source_file", "source_tag", "row_number", "src", "created_at", "imported_at"]
    insert_cols = [c for c in wanted if c in cols]

    if "import_id" not in cols:
        raise RuntimeError("stg_mls_raw must have import_id column")

    # Monta payload por linha
    payloads: List[Dict[str, Any]] = []
    for i, (_, r) in enumerate(df_raw.iterrows(), start=1):
        d = _safe_payload(r.to_dict())

        row: Dict[str, Any] = {"import_id": import_id}

        if "source_file" in cols:
            row["source_file"] = source_file
        if "source_tag" in cols:
            row["source_tag"] = source_tag
        if "row_number" in cols:
            row["row_number"] = i
        if "src" in cols:
            # ✅ manda dict mesmo (json object), NÃO string
            row["src"] = d
        # Se existirem colunas de timestamp, deixa o banco default se houver.
        # (não adicionamos aqui pra não errar tipos/nomes)

        payloads.append({k: row.get(k) for k in insert_cols})

    if not insert_cols:
        # Sem colunas além de import_id? então insere 1 linha só.
        sql = text(f'INSERT INTO {schema}.stg_mls_raw (import_id) VALUES (:import_id) ON CONFLICT (import_id) DO NOTHING;')
        with engine.begin() as conn:
            conn.execute(sql, {"import_id": import_id})
        return 1

    # Monta SQL dinâmico
    col_list = ", ".join(insert_cols)
    val_list = ", ".join([f":{c}" for c in insert_cols])

    # Se tiver row_number, conflito pode ser (import_id,row_number) dependendo do seu schema
    # Como não sabemos, vamos fazer sem ON CONFLICT e deixar o erro mostrar constraint certa.
    sql = text(f"INSERT INTO {schema}.stg_mls_raw ({col_list}) VALUES ({val_list});")

    with engine.begin() as conn:
        conn.execute(sql, payloads)

    return len(payloads)


def _insert_stg_mls_classified(
    engine: Engine,
    import_id: str,
    df_classified: pd.DataFrame,
    schema: str = "public",
) -> int:
    """
    Insere no stg_mls_classified:
    - Adiciona import_id
    - Só insere colunas que existem na tabela
    """
    cols = _table_columns(engine, "stg_mls_classified", schema=schema)
    if "import_id" not in cols:
        raise RuntimeError("stg_mls_classified must have import_id column")

    df = df_classified.copy()
    df["import_id"] = import_id

    # Normaliza colunas para bater com DB
    df_cols = [c for c in df.columns if c in cols]
    df = df[df_cols]

    # Converte NaN -> None
    df = df.where(pd.notnull(df), None)

    if df.empty:
        return 0

    col_list = ", ".join(df_cols)
    val_list = ", ".join([f":{c}" for c in df_cols])
    sql = text(f"INSERT INTO {schema}.stg_mls_classified ({col_list}) VALUES ({val_list});")

    payloads = df.to_dict(orient="records")
    with engine.begin() as conn:
        conn.execute(sql, payloads)

    return len(payloads)


# =========================================================
# Main ETL
# =========================================================

def run_etl(
    *,
    xlsx_file: Any,                 # Streamlit UploadedFile
    snapshot_date: date,
    contract_path: Union[str, Path],
    source_tag: str = "MLS",
    schema: str = "public",
) -> ETLResult:
    """
    Pipeline:
    1) salva UploadedFile em disco temporário
    2) lê XLSX para RAW (df_raw)
    3) grava RAW em stg_mls_raw (por linha, com row_number e src se existir)
    4) classifica via classify_xlsx() usando contract YAML
    5) grava CLASSIFIED em stg_mls_classified
    """
    try:
        engine = get_engine()

        # --- salva arquivo temporário
        if not hasattr(xlsx_file, "getbuffer"):
            return ETLResult(ok=False, error="xlsx_file must be a Streamlit UploadedFile")

        suffix = ".xlsx"
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(xlsx_file.getbuffer())
            tmp_path = Path(tmp.name)

        import_id = str(uuid.uuid4())
        source_file = tmp_path.name

        # --- lê raw
        df_raw = pd.read_excel(tmp_path, engine="openpyxl")

        # --- insere raw (resiliente)
        raw_inserted = _insert_stg_mls_raw(
            engine=engine,
            import_id=import_id,
            source_file=source_file,
            source_tag=source_tag,
            snapshot_date=snapshot_date,
            df_raw=df_raw,
            schema=schema,
        )

        # --- classifica
        contract_path = Path(contract_path)
        df_classified = classify_xlsx(
            xlsx_path=tmp_path,
            contract_path=contract_path,
            snapshot_date=snapshot_date,
        )

        # --- insere classified
        classified_inserted = _insert_stg_mls_classified(
            engine=engine,
            import_id=import_id,
            df_classified=df_classified,
            schema=schema,
        )

        return ETLResult(
            ok=True,
            import_id=import_id,
            rows_raw_inserted=raw_inserted,
            rows_classified_inserted=classified_inserted,
        )

    except Exception as e:
        return ETLResult(ok=False, error=str(e))
