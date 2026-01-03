import pandas as pd
import sqlalchemy as sa
from typing import List


# =========================
# 1. NORMALIZAÇÃO DE COLUNAS
# =========================

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Padroniza nomes de colunas para snake_case, lowercase,
    removendo caracteres problemáticos para SQL.
    """
    df = df.copy()

    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("/", "_", regex=False)
        .str.replace("-", "_", regex=False)
        .str.replace("__", "_", regex=False)
    )

    return df


# =========================
# 2. NORMALIZAÇÃO DE STATUS
# =========================

def normalize_status(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza status imobiliário para padrões internos.
    """
    df = df.copy()

    if "status" in df.columns:
        df["status"] = (
            df["status"]
            .str.strip()
            .str.upper()
        )

        df["status_norm"] = df["status"].map({
            "ACTIVE": "LISTING",
            "ACT": "LISTING",
            "PENDING": "PENDING",
            "PND": "PENDING",
            "SOLD": "SOLD",
            "SLD": "SOLD"
        }).fillna("OTHER")

    return df


# =========================
# 3. LIMPEZA DE NUMÉRICOS
# =========================

def coerce_numeric(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """
    Converte colunas para numérico com segurança.
    """
    df = df.copy()

    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# =========================
# 4. ETL PRINCIPAL
# =========================

def run_etl(
    df_raw: pd.DataFrame,
    engine: sa.Engine,
    project_id: str,
    table_name: str = "stg_properties"
) -> None:
    """
    Executa ETL completo:
    - normaliza colunas
    - normaliza status
    - força tipos
    - adiciona project_id
    - insere no banco
    """

    # 1) Normalizar colunas
    df = normalize_columns(df_raw)

    # 2) Normalizar status
    df = normalize_status(df)

    # 3) Forçar numéricos críticos
    numeric_cols = [
        "current_price",
        "heated_area",
        "beds",
        "full_baths",
        "half_baths",
        "year_built",
        "adom",
        "cdom",
        "lp_sqft",
        "sp_sqft",
        "sp_lp"
    ]

    df = coerce_numeric(df, numeric_cols)

    # 4) Project ID (controle absoluto de datasets)
    df["project_id"] = project_id

    # 5) Auditoria mínima
    if df.empty:
        raise ValueError("ETL abortado: dataframe vazio.")

    # 6) Insert no banco
    df.to_sql(
        table_name,
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000
    )
