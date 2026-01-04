from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional
from uuid import uuid4
import json

import pandas as pd
from sqlalchemy import text

from backend.db import get_engine
from backend.core.mls_classify import classify_xlsx


@dataclass
class ETLResult:
    import_id: str
    filename: str
    snapshot_date: str
    asset_class: str
    rows_classified: int
    rows_inserted: int
    summary: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "import_id": self.import_id,
            "filename": self.filename,
            "snapshot_date": self.snapshot_date,
            "asset_class": self.asset_class,
            "rows_classified": self.rows_classified,
            "rows_inserted": self.rows_inserted,
            "summary": self.summary,
        }


def _make_summary(df: pd.DataFrame) -> Dict[str, Any]:
    # Resumo robusto (não quebra se coluna não existir)
    def safe_value_counts(col: str, top: int = 10):
        if col not in df.columns:
            return {}
        vc = df[col].fillna("").astype(str).value_counts().head(top)
        return {k: int(v) for k, v in vc.items()}

    summary = {
        "status_group": safe_value_counts("status_group", 20),
        "status_raw": safe_value_counts("status_raw", 20),
        "zip_top": safe_value_counts("zip", 20),
        "city_top": safe_value_counts("city", 20),
        "county_top": safe_value_counts("county", 20),
        "price": {},
    }

    # price ranges
    for col in ["list_price", "close_price"]:
        if col in df.columns:
            s = pd.to_numeric(df[col], errors="coerce").dropna()
            if len(s) > 0:
                summary["price"][col] = {
                    "min": float(s.min()),
                    "p25": float(s.quantile(0.25)),
                    "median": float(s.median()),
                    "p75": float(s.quantile(0.75)),
                    "max": float(s.max()),
                }
            else:
                summary["price"][col] = {}

    return summary


def _insert_stg_mls_raw(engine, import_id: str) -> None:
    # Mantém o mínimo para não quebrar com schemas diferentes
    sql = text("""
        insert into stg_mls_raw (import_id, imported_at)
        values (:import_id, now())
        on conflict (import_id) do nothing;
    """)
    with engine.begin() as conn:
        conn.execute(sql, {"import_id": import_id})


def _insert_stg_mls_classified(engine, df: pd.DataFrame) -> int:
    # pandas to_sql (append)
    # garantir que import_id exista no DF e esteja como str
    inserted = 0
    with engine.begin() as conn:
        df.to_sql("stg_mls_classified", conn, if_exists="append", index=False, method="multi", chunksize=1000)
        inserted = len(df)
    return inserted


def _save_report(engine, result: ETLResult) -> None:
    sql = text("""
        insert into etl_import_reports (
          import_id, snapshot_date, filename, asset_class,
          rows_classified, rows_inserted, summary
        )
        values (
          :import_id::uuid, :snapshot_date::date, :filename, :asset_class,
          :rows_classified, :rows_inserted, :summary::jsonb
        )
        on conflict (import_id) do update set
          snapshot_date = excluded.snapshot_date,
          filename = excluded.filename,
          asset_class = excluded.asset_class,
          rows_classified = excluded.rows_classified,
          rows_inserted = excluded.rows_inserted,
          summary = excluded.summary;
    """)
    with engine.begin() as conn:
        conn.execute(sql, {
            "import_id": result.import_id,
            "snapshot_date": result.snapshot_date,
            "filename": result.filename,
            "asset_class": result.asset_class,
            "rows_classified": result.rows_classified,
            "rows_inserted": result.rows_inserted,
            "summary": json.dumps(result.summary),
        })


def run_etl(
    *,
    xlsx_path: str | Path,
    snapshot_date: date,
    contract_path: str | Path,
    preview_rows: int = 0,
) -> Dict[str, Any]:
    """
    Executa o pipeline:
    - gera import_id UUID
    - grava stg_mls_raw
    - classifica XLSX -> df_classified
    - injeta import_id em cada linha
    - grava stg_mls_classified
    - gera e salva report em etl_import_reports
    Retorna dict com resumo (e preview opcional).
    """
    engine = get_engine()

    xlsx_path = Path(xlsx_path)
    contract_path = Path(contract_path)

    import_id = str(uuid4())

    # 1) cria a "raiz" no raw (para FK)
    _insert_stg_mls_raw(engine, import_id)

    # 2) classifica
    df = classify_xlsx(xlsx_path=xlsx_path, contract_path=contract_path, snapshot_date=snapshot_date)

    # 3) injeta import_id (FK)
    df.insert(0, "import_id", import_id)

    asset_class = str(df["asset_class"].iloc[0]) if "asset_class" in df.columns and len(df) else "unknown"
    summary = _make_summary(df)

    # 4) grava classified
    rows_inserted = _insert_stg_mls_classified(engine, df)

    # 5) salva report
    result = ETLResult(
        import_id=import_id,
        filename=xlsx_path.name,
        snapshot_date=str(snapshot_date),
        asset_class=asset_class,
        rows_classified=len(df),
        rows_inserted=rows_inserted,
        summary=summary,
    )
    _save_report(engine, result)

    payload = result.to_dict()

    # preview opcional
    if preview_rows and preview_rows > 0:
        payload["preview"] = df.head(preview_rows).to_dict(orient="records")

    return payload
