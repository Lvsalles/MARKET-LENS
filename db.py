import psycopg2
from psycopg2.extras import execute_values


def bulk_insert_dicts(
    conn,
    table_name: str,
    rows: list[dict],
    allowed_columns: list[str]
) -> int:
    """
    Bulk insert a list of dicts into a Postgres table.

    - conn: psycopg2 connection
    - table_name: target table
    - rows: list of dictionaries
    - allowed_columns: explicit ordered list of columns
    """

    if not rows:
        return 0

    # Enforce column order & safety
    records = []
    for row in rows:
        records.append(
            tuple(row.get(col) for col in allowed_columns)
        )

    columns_sql = ", ".join(allowed_columns)
    values_sql = ", ".join(["%s"] * len(allowed_columns))

    insert_sql = f"""
        INSERT INTO {table_name} ({columns_sql})
        VALUES %s
    """

    with conn.cursor() as cur:
        execute_values(
            cur,
            insert_sql,
            records,
            template=f"({values_sql})"
        )

    conn.commit()
    return len(records)
