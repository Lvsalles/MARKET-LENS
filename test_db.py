from db import engine

with engine.connect() as conn:
    result = conn.execute("SELECT now()")
    print("CONNECTED:", result.fetchone())
