import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not found in environment")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

def get_engine():
    return engine
