from sqlalchemy import text
from db import get_engine


def insert_into_staging(df, project_id, category):
    engine = get_engine()

    with engine.begin() as conn:

        # 1️⃣ Remove dados antigos do mesmo projeto + categoria
        delete_sql = text("""
            DELETE FROM stg_mls
            WHERE project_id = :project_id
              AND category = :category
        """)
        conn.execute(delete_sql, {
            "project_id": project_id,
            "category": category
        })

        # 2️⃣ Insere os novos dados
        df["project_id"] = project_id
        df["category"] = category

        df.to_sql(
            "stg_mls",
            con=conn,
            if_exists="append",
            index=False,
            method="multi"
        )
