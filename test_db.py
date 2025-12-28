from sqlalchemy import create_engine

DATABASE_URL = "postgresql://postgres.kzkxqsivqymdtmtyzmqh:G%40bys2010@aws-0-us-west-2.pooler.supabase.com:6543/postgres"

engine = create_engine(DATABASE_URL)

with engine.connect() as conn:
    result = conn.execute("SELECT now()")
    print("CONNECTED:", result.fetchone())
