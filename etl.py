def normalize_columns(df):
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )

    # Normalizar status
    if "status" in df.columns:
        df["status"] = df["status"].astype(str).str.upper()
    elif "status_name" in df.columns:
        df["status"] = df["status_name"].astype(str).str.upper()
    else:
        df["status"] = "UNKNOWN"

    # Criar coluna category (CRÍTICO)
    def infer_category(row):
        s = row["status"]

        if "SOLD" in s or "CLOSED" in s:
            return "Sold"
        if "ACTIVE" in s or "ACTIVE UNDER CONTRACT" in s:
            return "Listings"
        if "RENT" in s or "LEASE" in s:
            return "Rental"
        if "LAND" in s or "LOT" in s:
            return "Land"
        return "Other"

    df["category"] = df.apply(infer_category, axis=1)

    return df
