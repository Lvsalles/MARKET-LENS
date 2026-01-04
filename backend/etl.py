# backend/etl.py
import pandas as pd
from sqlalchemy import text
from backend.db import get_engine
from backend.contracts.mls_classify import classify_xlsx

def run_etl(xlsx_file, snapshot_date):
    """
    Pipeline ETL MLS:
    1) Lê XLSX
    2) Insere RAW (gera import_id UUID no banco)
    3) Reutiliza o MESMO import_id no CLASSIFIED
    """

    engine = get_engine()

    # --- 1. Ler arquivo ---
    raw_df = pd.read_excel(xlsx_file)

    if raw_df.empty:
        raise ValueError("Arquivo XLSX está vazio")

    with engine.begin() as conn:
        # --- 2. Inserir RAW ---
        raw_df["snapshot_date"] = snapshot_date

        raw_df.to_sql(
            "stg_mls_raw",
            conn,
            if_exists="append",
            index=False,
            method="multi",
        )

        # --- 3. Capturar import_id recém-criado ---
        result = conn.execute(
            text(
                """
                SELECT import_id
                FROM stg_mls_raw
                ORDER BY created_at DESC
                LIMIT 1
                """
            )
        ).fetchone()

        if not result:
            raise RuntimeError("Falha ao capturar import_id do RAW")

        import_id = result[0]

        # --- 4. Classificar / normalizar ---
        classified_df = classify_xlsx(raw_df)

        if classified_df.empty:
            raise ValueError("Classificação retornou DataFrame vazio")

        classified_df["snapshot_date"] = snapshot_date
        classified_df["import_id"] = import_id

        # --- 5. Inserir CLASSIFIED ---
        classified_df.to_sql(
            "stg_mls_classified",
            conn,
            if_exists="append",
            index=False,
            method="multi",
        )

    return {
        "status": "success",
        "rows_raw": len(raw_df),
        "rows_classified": len(classified_df),
        "import_id": str(import_id),
    }
