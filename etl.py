from __future__ import annotations
import io
import uuid
import pandas as pd
import numpy as np
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.engine import Engine

def _first_day_of_month(dt: pd.Series) -> pd.Series:
    d = pd.to_datetime(dt, errors="coerce")
    return d.dt.to_period("M").dt.to_timestamp()

def normalize_df(df: pd.DataFrame, category: str) -> pd.DataFrame:
    """
    Ajuste aqui o mapeamento de colunas do MLS para os nomes canônicos.
    Você vai evoluir isso conforme suas planilhas reais.
    """
    # Normaliza nomes de colunas
    cols = {c: c.strip() for c in df.columns}
    df = df.rename(columns=cols)

    # Tenta mapear automaticamente algumas colunas comuns
    def pick(*names):
        for n in names:
            if n in df.columns:
                return n
        return None

    col_mls_id = pick("MLS #", "MLS", "ListingId", "Listing ID", "MlsId", "MlsNumber")
    col_address = pick("Address", "Street Address", "Full Address")
    col_city = pick("City")
    col_zip = pick("Zip", "ZIP", "Zip Code", "PostalCode", "Postal Code")
    col_price = pick("List Price", "Price", "ListPrice")
    col_sold_price = pick("Sold Price", "Close Price", "SoldPrice", "ClosePrice")
    col_sqft = pick("SqFt", "Living Area", "LivingArea", "Living Area SqFt", "Heated SqFt")
    col_beds = pick("Beds", "Bedrooms", "BR")
    col_baths = pick("Baths", "Bathrooms", "BA")
    col_garage = pick("Garage", "Garage Spaces", "GarageSpaces")
    col_dom = pick("DOM", "CDOM", "Days on Market")
    col_adom = pick("ADOM", "Adjusted DOM")
    col_list_date = pick("List Date", "ListDate")
    col_sold_date = pick("Sold Date", "Close Date", "CloseDate", "SoldDate")

    col_financing = pick("Financing", "Financing Type", "FinancingType")
    col_prop_type = pick("Property Type", "PropertyType")
    col_prop_sub = pick("Property Subtype", "PropertySubType", "Subtype")

    col_list_agent = pick("List Agent", "ListAgent", "List Agent Full Name", "ListAgentFullName")
    col_sell_agent = pick("Sell Agent", "Selling Agent", "Cooperating Agent", "SellAgentFullName", "SellingAgentFullName")
    col_list_office = pick("List Office", "ListOffice", "List Office Name", "ListOfficeName")
    col_sell_office = pick("Sell Office", "Selling Office", "SellOfficeName")

    out = pd.DataFrame()
    out["mls_id"] = df[col_mls_id] if col_mls_id else None
    out["address"] = df[col_address] if col_address else None
    out["city"] = df[col_city] if col_city else None
    out["zipcode"] = df[col_zip] if col_zip else None

    out["category"] = category
    out["status"] = category  # opcional: você pode mapear ACT/PND/SLD depois
    out["property_type"] = df[col_prop_type] if col_prop_type else None
    out["property_subtype"] = df[col_prop_sub] if col_prop_sub else None

    out["price"] = pd.to_numeric(df[col_price], errors="coerce") if col_price else np.nan
    out["sold_price"] = pd.to_numeric(df[col_sold_price], errors="coerce") if col_sold_price else np.nan
    out["sqft"] = pd.to_numeric(df[col_sqft], errors="coerce") if col_sqft else np.nan

    out["beds"] = pd.to_numeric(df[col_beds], errors="coerce") if col_beds else np.nan
    out["baths"] = pd.to_numeric(df[col_baths], errors="coerce") if col_baths else np.nan
    out["garage"] = pd.to_numeric(df[col_garage], errors="coerce") if col_garage else np.nan

    out["dom"] = pd.to_numeric(df[col_dom], errors="coerce") if col_dom else np.nan
    out["adom"] = pd.to_numeric(df[col_adom], errors="coerce") if col_adom else out["dom"]

    out["list_date"] = pd.to_datetime(df[col_list_date], errors="coerce").dt.date if col_list_date else None
    out["sold_date"] = pd.to_datetime(df[col_sold_date], errors="coerce").dt.date if col_sold_date else None

    out["financing"] = df[col_financing] if col_financing else None

    out["list_agent"] = df[col_list_agent] if col_list_agent else None
    out["sell_agent"] = df[col_sell_agent] if col_sell_agent else None
    out["list_office"] = df[col_list_office] if col_list_office else None
    out["sell_office"] = df[col_sell_office] if col_sell_office else None

    # Derived metrics
    out["ppsqft"] = np.where(out["sqft"] > 0, out["price"] / out["sqft"], np.nan)
    out["sp_psqft"] = np.where(out["sqft"] > 0, out["sold_price"] / out["sqft"], np.nan)

    # Month key: prefer sold_date for Sold, otherwise list_date
    base_date = out["sold_date"] if category.lower() == "sold" else out["list_date"]
    out["month_key"] = _first_day_of_month(base_date)

    # Clean zipcode
    out["zipcode"] = out["zipcode"].astype(str).str.extract(r"(\d{5})", expand=False)

    return out

def create_dataset(engine: Engine, project_id: str, filename: str, category: str, source: str = "MLS") -> str:
    dataset_id = str(uuid.uuid4())
    with engine.begin() as conn:
        conn.execute(
            text("""
                insert into datasets(id, project_id, filename, source, category, status)
                values (:id, :project_id, :filename, :source, :category, 'processing')
            """),
            dict(id=dataset_id, project_id=project_id, filename=filename, source=source, category=category)
        )
    return dataset_id

def insert_raw_records(engine: Engine, dataset_id: str, df: pd.DataFrame):
    rows = []
    for i, rec in enumerate(df.to_dict(orient="records"), start=1):
        rows.append({"dataset_id": dataset_id, "row_num": i, "raw": rec})

    with engine.begin() as conn:
        conn.execute(
            text("""
                insert into raw_records(dataset_id, row_num, raw)
                values (:dataset_id, :row_num, cast(:raw as jsonb))
            """),
            rows
        )

def insert_normalized(engine: Engine, project_id: str, dataset_id: str, norm: pd.DataFrame):
    payload = norm.copy()
    payload["project_id"] = project_id
    payload["dataset_id"] = dataset_id

    # SQL insert bulk
    cols = [
        "project_id","dataset_id","mls_id","address","city","state","zipcode",
        "list_date","pending_date","sold_date",
        "category","status","property_type","property_subtype",
        "price","sold_price","sqft","lot_sqft",
        "beds","baths","garage","year_built","dom","adom",
        "financing","hoa","list_agent","list_office","sell_agent","sell_office",
        "ppsqft","sp_psqft","month_key"
    ]
    for c in cols:
        if c not in payload.columns:
            payload[c] = None

    records = payload[cols].to_dict(orient="records")

    with engine.begin() as conn:
        conn.execute(
            text(f"""
                insert into normalized_properties (
                  {",".join(cols)}
                ) values (
                  {",".join([f":{c}" for c in cols])}
                )
            """),
            records
        )

    with engine.begin() as conn:
        conn.execute(
            text("""
                update datasets
                set status='completed', record_count=:cnt
                where id=:dataset_id
            """),
            dict(cnt=len(records), dataset_id=dataset_id)
        )

def load_excel_bytes(file_bytes: bytes, sheet_name: str | None = None) -> pd.DataFrame:
    bio = io.BytesIO(file_bytes)
    df = pd.read_excel(bio, sheet_name=sheet_name)
    if isinstance(df, dict):
        # se vier múltiplas abas, pega a primeira
        df = list(df.values())[0]
    return df

def ingest_excel(engine: Engine, project_id: str, file_bytes: bytes, filename: str, category: str, sheet_name: str | None = None):
    df = load_excel_bytes(file_bytes, sheet_name=sheet_name)

    dataset_id = create_dataset(engine, project_id, filename, category)

    # raw
    insert_raw_records(engine, dataset_id, df)

    # normalized
    norm = normalize_df(df, category)
    insert_normalized(engine, project_id, dataset_id, norm)

    return dataset_id, len(df)
