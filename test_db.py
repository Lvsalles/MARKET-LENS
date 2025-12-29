from sqlalchemy import create_engine, text

engine = create_engine("postgresql://postgres:G%40bys2010@db.xxxxx.supabase.co:5432/postgres")

with engine.connect() as conn:
    print(conn.execute(text("SELECT current_database(), current_user")).fetchone())
