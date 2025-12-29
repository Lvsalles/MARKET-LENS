import re
import pandas as pd
import streamlit as st
from sqlalchemy import text, inspect
from db import get_engine

# =========================
# CONFIG (TEM QUE SER PRIMEIRO)
# =========================
st.set_page_config(page_title="Market Lens", layout="wide")

st.title("📊 Market Lens — Upload de Dados (Staging)")

# =========================
# HELPERS
# =========================

MAX_FILES = 12

def guess_category_from_filename(filename: str) -> str:
    """
    Tenta identificar a categoria a partir do nome do arquivo.
    Você pode melhorar as regras depois.
    """
    name = filename.lower()

    if "land" in name:
        return "Land"
    if "rental" in name or "rent" in name:
        return "Rental"
    if "pending" in name or "pendings" in name or "pnd" in name:
        return "Pendings"
    if "sold" in name or "sld" in name:
        return "Sold"
    if "listing" in name or "listings" in name or "act" in name:
        return "Listings"
    if "propriedade" in name or "propriedades" in name:
        # normalmente esse arquivo é um mix — você pode escolher como tratar
        # aqui vamos mandar pra Listings por padrão, mas você pode mudar.
        return "Listings"

    return "Listings"


def sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza nomes de colunas vindas do Excel.
    """
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", "_", regex=True)
        .str.replace("-", "_")
        .str.replace("/", "_")
        .str.replace(r"[^a-z0-9_]+", "", regex=True)
    )
    return df


def map_common_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Mapeia colunas comuns de MLS -> schema do stg_mls.
    Se uma coluna não existir, tudo bem.
    """
    df = df.copy()

    # mapeamentos comuns (adicione mais quando quiser)
    rename_map = {
        "#": "row_no",
        "ml_number": "mls_id",
        "mls_number": "mls_id",
        "mls": "mls_id",
        "listing_number": "mls_id",
        "address": "address",
        "street_name": "street",
        "street": "street",
        "city": "city",
        "state": "state",
        "zip": "zipcode",
        "zip_code": "zipcode",
        "zipcode": "zipcode",
        "county": "county",
        "legal_subdivision_name": "subdivision",
        "subdivision": "subdivision",
        "subdivision_condo_name": "subdivision",
        "heated_area": "sqft",
        "living_area": "sqft",
        "sq_ft": "sqft",
        "sqft": "sqft",
        "lot_size": "lot_sqft",
        "lot_sqft": "lot_sqft",
        "total_acreage": "total_acreage",  # se não existir no banco, será filtrado depois
        "current_price": "list_price",
        "list_price": "list_price",
        "sold_price": "sold_price",
        "close_price": "sold_price",
        "beds": "beds",
        "bedrooms": "beds",
        "full_baths": "baths",
        "baths_full": "baths",
        "baths": "baths",
        "garage": "garage",
        "pool": "pool",
        "year_built": "year_built",
        "dom": "dom",
        "adom": "adom",
        "cdom": "cdom",
        "list_date": "list_date",
        "pending_date": "pending_date",
        "sold_date": "sold_date",
        "sold_terms": "financing",
        "financing": "financing",
        "list_agent": "list_agent",
        "listing_agent": "list_agent",
        "sell_agent": "sell_agent",
        "selling_agent": "sell_agent",
        "latitude": "latitude",
        "longitude": "longitude",
        "lat": "latitude",
        "lng": "longitude",
    }

    for c in list(df.columns):
        if c in rename_map and rename_map[c] != c:
            df = df.rename(columns={c: rename_map[c]})

    return df


def ensure_month_key(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria month_key = YYYY-MM-01 para análises MoM/YoY depois.
    Regra simples:
      - Sold -> usa sold_date
      - Pending -> usa pending_date
      - Listings -> usa list_date
      - fallback: hoje
    """
    df = df.copy()

    # tenta converter datas se existirem
    for col in ["list_date", "pending_date", "sold_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    # cria month_key se faltar
    if "month_key" not in df.columns:
        df["month_key"] = pd.NaT

    # prioriza sold_date, depois pending_date, depois list_date
    if "sold_date" in df.columns:
        sold_dt = pd.to_datetime(df["sold_date"], errors="coerce")
        df.loc[df["month_key"].isna(), "month_key"] = sold_dt.dt.to_period("M").dt.to_timestamp().dt.date

    if "pending_date" in df.columns:
        pnd_dt = pd.to_datetime(df["pending_date"], errors="coerce")
        df.loc[df["month_key"].isna(), "month_key"] = pnd_dt.dt.to_period("M").dt.to_timestamp().dt.date

    if "list_date" in df.columns:
        lst_dt = pd.to_datetime(df["list_date"], errors="coerce")
        df.loc[df["month_key"].isna(), "month_key"] = lst_dt.dt.to_period("M").dt.to_timestamp().dt.date

    # fallback: mês atual
    today = pd.Timestamp.today()
    df.loc[df["month_key"].isna(), "month_key"] = today.to_period("M").to_timestamp().date()

    return df


def cast_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte tipos básicos sem quebrar.
    """
    df = df.copy()

    numeric_cols = [
        "beds", "baths", "garage", "sqft", "lot_sqft",
        "list_price", "sold_price", "dom", "adom", "cdom",
        "latitude", "longitude", "sp_lp", "ppsqft", "total_acreage"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "pool" in df.columns:
        # aceita True/False, Y/N, Yes/No, 1/0
        df["pool"] = df["pool"].astype(str).str.strip().str.lower().map(
            {"true": True, "false": False, "y": True, "n": False, "yes": True, "no": False, "1": True, "0": False}
        )

    if "year_built" in df.columns:
        df["year_built"] = pd.to_numeric(df["year_built"], errors="coerce").astype("Int64")

    # calcula sp_lp e ppsqft quando possível
    if "sold_price" in df.columns and "list_price" in df.columns:
        df["sp_lp"] = df["sold_price"] / df["list_price"]

    if "sqft" in df.columns:
        i
