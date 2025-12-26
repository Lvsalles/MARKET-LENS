import re

def normalize_column(col: str) -> str:
    col = col.lower().strip()
    col = re.sub(r"[^a-z0-9]+", "_", col)
    return col.strip("_")


CANONICAL_SCHEMA = {
    # Identifiers
    "ml_number": [
        "ml_number", "ml_number", "mls_number", "listing_number", "listing_id"
    ],
    "status": ["status", "mls_status", "listing_status"],

    # Location
    "address": ["address", "full_address", "street_address"],
    "city": ["city"],
    "county": ["county", "county_or_parish"],
    "zip": ["zip", "zipcode", "postal_code"],

    # Prices
    "price": ["price", "current_price", "list_price"],
    "tax": ["tax", "annual_tax"],

    # Size
    "sqft": ["sqft", "heated_area", "living_area"],
    "acreage": ["acreage", "acres", "lot_acres"],
    "lot_sqft": ["lot_sqft", "lot_size", "lot_size_sqft"],

    # Features
    "beds": ["beds", "bedrooms"],
    "full_baths": ["full_baths", "full_bathrooms", "bathrooms"],
    "half_baths": ["half_baths"],
    "year_built": ["year_built", "yearbuilt"],
    "pool": ["pool", "has_pool"],

    # Agent / Office
    "agent_name": ["list_agent", "agent_name", "agent"],
    "agent_id": ["list_agent_id", "agent_id", "license"],
    "office_name": ["list_office", "office_name", "brokerage"],
    "office_id": ["list_office_id", "office_id"],
}


def apply_schema_dictionary(df):
    normalized_cols = {normalize_column(c): c for c in df.columns}
    reverse_map = {}

    for canonical, aliases in CANONICAL_SCHEMA.items():
        for alias in aliases:
            alias_norm = normalize_column(alias)
            if alias_norm in normalized_cols:
                reverse_map[normalized_cols[alias_norm]] = canonical
                break

    return df.rename(columns=reverse_map)
