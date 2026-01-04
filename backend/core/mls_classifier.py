# backend/core/mls_classifier.py
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from backend.core.normalization import clean_string, to_numeric, to_date


def infer_asset_class(columns: List[str], contract: dict) -> str:
    cols = set(columns)

    if set(contract["signatures"]["rental_if_has_any"]).intersection(cols):
        return "rental"

    if set(contract["signatures"]["land_if_has_any"]).intersection(cols):
        return "land"

    return "residential_sale"


def map_status(asset_class: str, status_raw: str, contract: dict):
    status_raw = clean_string(status_raw)
    if not status_raw:
        raise ValueError("Status is missing")

    rules = contract["status_rules"][asset_class]
    mapped = rules.get(status_raw)

    if not mapped:
        raise ValueError(f"Unmapped status '{status_raw}' for asset_class '{asset_class}'")

    if mapped.startswith("closed:"):
        return "closed", mapped.split(":")[1]

    return mapped, None


def normalize_price(asset_class, status_raw, current_price):
    status_raw = clean_string(status_raw)
    price = to_numeric(current_price)

    if asset_class in {"land", "residential_sale"}:
        return (None, price) if status_raw == "SLD" else (price, None)

    if asset_class == "rental":
        return (None, price) if status_raw == "LSE" else (price, None)

    return None, None


def classify_dataframe(
    df: pd.DataFrame,
    contract: dict,
    snapshot_date: Optional[date] = None,
) -> pd.DataFrame:
    snapshot_date = snapshot_date or date.today()
    df.columns = [clean_string(c) for c in df.columns]

    asset_class = infer_asset_class(df.columns.tolist(), contract)
    rows: List[Dict[str, Any]] = []

    for _, r in df.iterrows():
        status_raw = clean_string(r.get("Status"))
        status_group, closed_type = map_status(asset_class, status_raw, contract)
        list_price, close_price = normalize_price(asset_class, status_raw, r.get("Current Price"))

        rows.append({
            "snapshot_date": snapshot_date,
            "asset_class": asset_class,
            "status_raw": status_raw,
            "status_group": status_group,
            "closed_type": closed_type,
            "ml_number": clean_string(r.get("ML Number")),
            "address": clean_string(r.get("Address")),
            "city": clean_string(r.get("City")),
            "zip": clean_string(r.get("Zip")),
            "county": clean_string(r.get("County")),
            "legal_subdivision_name": clean_string(r.get("Legal Subdivision Name")),
            "subdivision_condo_name": clean_string(r.get("Subdivision/Condo Name")),
            "property_style_raw": clean_string(r.get("Property Style")),
            "property_subtype": "Vacant Land" if asset_class == "land" else clean_string(r.get("Property Style")),
            "list_agent": clean_string(r.get("List Agent")),
            "list_agent_id": clean_string(r.get("List Agent ID")),
            "selling_office_id": clean_string(r.get("Selling Office ID")),
            "list_office_id": clean_string(r.get("List Office ID")),
            "list_office_name": clean_string(r.get("List Office")),
            "list_office_board_id": clean_string(r.get("List Office Primary Board ID")),
            "list_price": list_price,
            "close_price": close_price,
            "close_date": to_date(r.get("Close Date")),
            "beds": to_numeric(r.get("Beds")),
            "full_baths": to_numeric(r.get("Full Baths")),
            "half_baths": to_numeric(r.get("Half Baths")),
            "heated_area": to_numeric(r.get("Heated Area")),
            "year_built": to_numeric(r.get("Year Built")),
            "pool": clean_string(r.get("Pool")),
            "pets_allowed": clean_string(r.get("Pets Allowed")),
            "lease_amount_frequency": clean_string(r.get("Lease Amount Frequency")),
            "date_available": to_date(r.get("Date Available")),
            "lot_dimensions": clean_string(r.get("Lot Dimensions")),
            "lot_size_sqft": to_numeric(r.get("Lot Size Square Footage")),
            "total_acreage": to_numeric(r.get("Total Acreage")),
            "zoning": clean_string(r.get("Zoning")),
            "ownership": clean_string(r.get("Ownership")),
            "tax": to_numeric(r.get("Tax")),
            "adom": to_numeric(r.get("ADOM")),
            "cdom": to_numeric(r.get("CDOM")),
            "days_to_contract": to_numeric(r.get("Days to Contract")),
            "sold_terms": clean_string(r.get("Sold Terms")),
            "lp_sqft": to_numeric(r.get("LP / SqFt")),
            "sp_sqft": to_numeric(r.get("SP/SqFt")),
            "sp_lp": to_numeric(r.get("SP / LP")),
            "lsc_list_side": clean_string(r.get("LSC List Side")),
        })

    return pd.DataFrame(rows)
