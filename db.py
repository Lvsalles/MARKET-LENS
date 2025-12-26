from sqlalchemy import create_engine
import os

DATABASE_URL = os.environ["SUPABASE_DB_URL"]

engine = create_engine(DATABASE_URL)
