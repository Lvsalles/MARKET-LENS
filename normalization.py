from __future__ import annotations
import pandas as pd
import numpy as np
import re
from datetime import datetime


def _pick(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = {c.lower().strip(): c for c in df.columns}
    for cand in candidates:
        key = cand.lower().strip()
        if key in cols:
            return cols[key]
    return None


def _zip5(series: pd.Series) -> pd.Series:
    s = series.astype(str)
    return s.str.extract(r"(\d{5})", expand=False)


def normalize_mls_dataframe(df: pd.DataFrame, category: str) -> pd.DataFrame:
    """
    Converte uma planilha MLS para colunas canônicas usadas no banco.
    Você pode evoluir os candidates conforme seus exports.
    """
    df = df.copy()

    # Candidates (adicione conforme seus relatórios)
    c_mls = _pick(df, ["mls", "mls #", "mls id", "listing id", "listingid", "mlsnumber"])
    c_addr = _pick(df, ["address", "full address", "street address"])
    c_city = _pick(df, ["city"])
    c_zip = _pick(df, ["zip", "zip code", "zipcode", "postalcode", "postal code"])
    c_price = _pick(df, ["list price", "price", "listprice"])
    c_sold_price = _pick(df, ["sold price", "close price", "soldprice", "closeprice"])
    c_sqft = _pick(df, ["sqft", "living area", "livingarea", "heated sqft", "living area sqft"])
    c_beds = _pick(df, ["beds", "bedrooms", "br"])
    c_baths = _pick(df, ["baths", "bathrooms", "ba"])
    c_garage = _pick(df, ["garage", "garage spaces", "garagespaces"])
    c_adom = _pick(df, ["adom", "adjusted dom"])
    c_dom = _pick(df, ["dom", "cdom", "days on market"])
    c_list_date = _pick(df, ["list date", "listdate"])
    c_sold_date = _pick(df, ["sold date", "close date", "closedate", "solddate"])
    c_fin = _pick(df, ["financing", "financing type", "financingtype"])
    c_type = _pick(df, ["property type", "propertytype"])
    c_subtype = _pick(df, ["property subtype", "propertysubtype", "subtype"])
    c_list_agent = _pick(df, ["list agent", "listagent", "list agent full name"])
    c_sell_agent = _pick(df, ["sell agent", "selling agent", "sellagent", "selling agent full name"])

    out = pd.DataFrame(index=df.index)
    out["category"] = category

    out["mls_id"] = df[c_mls] if c_mls else None
    out["address"] = df[c_addr] if c_addr else None
    out["city"] = df[c_city] if c_city else None
    out["zipcode"] = _zip5(df[c_zip]) if c_zip else None

    out["property_type"] = df[c_type] if c_type else None
    out["property_subtype"] = df[c_subtype] if c_subtype else None
    out["financing"] = df[c_fin] if c_fin else None

    out["price"] = pd.to_numeric(df[c_price], errors="coerce") if c_price else np.nan
    out["sold_price"] = pd.to_numeric(df[c_sold_price], errors="coerce") if c_sold_price else np.nan
    out["sqft"] = pd.to_numeric(df[c_sqft], errors="coerce") if c_sqft else np.nan

    out["beds"] = pd.to_numeric(df[c_beds], errors="coerce") if c_beds else np.nan
    out["baths"] = pd.to_numeric(df[c_baths], errors="coerce") if c_baths else np.nan
    out["garage"] = pd.to_numeric(df[c_garage], errors="coerce") if c_garage else np.nan

    out["dom"] = pd.to_numeric(df[c_dom], errors="coerce") if c_dom else np.nan
    out["adom"] = pd.to_numeric(df[c_adom], errors="coerce") if c_adom else out["dom"]

    # datas
    out["list_date"] = pd.to_datetime(df[c_list_date], errors="coerce").dt.date if c_list_date else None
    out["sold_date"] = pd.to_datetime(df[c_sold_date], errors="coerce").dt.date if c_sold_date else None

    # derivados
    out["ppsqft"] = np.where(out["sqft"] > 0, out["price"] / out["sqft"], np.nan)
    out["month_key"] = pd.to_datetime(
        out["sold_date"] if category.lower() == "sold" else out["list_date"],
        errors="coerce"
    ).to_period("M").dt.to_timestamp().dt.date

    out["list_agent"] = df[c_list_agent] if c_list_agent else None
    out["sell_agent"] = df[c_sell_agent] if c_sell_agent else None

    return out
