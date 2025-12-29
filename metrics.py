def read_stg(engine, project_id: str, category: str | None = None):
    """
    category:
      - 'Sold'
      - 'Listings'
      - 'Pending'
      - 'Rental'
      - 'Land'
    """
    base_query = """
        SELECT *
        FROM stg_mls
        WHERE project_id = :project_id
    """

    filters = []
    params = {"project_id": project_id}

    if category == "Sold":
        filters.append("status ILIKE '%SOLD%' OR status ILIKE '%CLOSED%'")

    elif category == "Listings":
        filters.append("status ILIKE '%ACTIVE%'")

    elif category == "Pending":
        filters.append("status ILIKE '%PENDING%' OR status ILIKE '%CONTINGENT%'")

    elif category == "Rental":
        filters.append("status ILIKE '%RENT%'")

    elif category == "Land":
        filters.append("property_type ILIKE '%LAND%' OR property_type ILIKE '%LOT%'")

    if filters:
        base_query += " AND (" + " OR ".join(filters) + ")"

    with engine.begin() as conn:
        return pd.read_sql(base_query, conn, params=params)
