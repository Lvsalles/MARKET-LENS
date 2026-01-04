import os
import json
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
import google.generativeai as genai

# =========================
# CONFIG
# =========================
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

MODEL_NAME = "models/gemini-1.5-flash"

SYSTEM_PROMPT = """
You are an institutional real estate data classifier.

Given one MLS row, classify:
1) asset_class: RESIDENTIAL, RENTAL, or LAND
2) status_norm: LISTING, PENDING, SOLD, or LEASED

Rules:
- Use explicit signals first (status codes, property_style).
- LAND if no beds/baths AND style mentions land/lot/vacant.
- RENTAL if status or style mentions lease/rent/LSE.
- Otherwise RESIDENTIAL.
- Return JSON only.
"""

# =========================
# AI CALL
# =========================
def classify_row(row: dict) -> dict:
    model = genai.GenerativeModel(MODEL_NAME)

    prompt = SYSTEM_PROMPT + "\n\nMLS ROW:\n" + json.dumps(row, default=str)

    resp = model.generate_content(prompt)
    text_out = resp.text.strip()

    try:
        data = json.loads(text_out)
        return {
            "ai_asset_class": data.get("asset_class"),
            "ai_status_norm": data.get("status_norm"),
            "ai_confidence": float(data.get("confidence", 0.5)),
        }
    except Exception:
        return {
            "ai_asset_class": None,
            "ai_status_norm": None,
            "ai_confidence": 0.0,
        }

# =========================
# APPLY TO DB
# =========================
def run_ai_classification(engine: Engine, limit: int = 500):
    fetch_sql = text("""
        select *
        from stg_mls
        where ai_asset_class is null
        limit :limit
    """)

    update_sql = text("""
        update stg_mls
        set
          ai_asset_class = :ai_asset_class,
          ai_status_norm = :ai_status_norm,
          ai_confidence = :ai_confidence
        where id = :id
    """)

    with engine.connect() as conn:
        df = pd.read_sql(fetch_sql, conn, params={"limit": limit})

        if df.empty:
            return "No rows to classify."

        for _, r in df.iterrows():
            result = classify_row(r.to_dict())

            conn.execute(
                update_sql,
                {
                    "id": r["id"],
                    **result,
                },
            )
        conn.commit()

    return f"Classified {len(df)} rows."
