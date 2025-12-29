from sqlalchemy import create_engine, text
import os

engine = create_engine(os.environ["DATABASE_URL"])

with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'stg_mls'
        ORDER BY column_name;
    """))

    print("COLUNAS DA TABELA stg_mls:")
    for r in result:
        print("-", r[0])
