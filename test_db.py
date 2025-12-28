import os
import sys
import streamlit as st
from sqlalchemy import text

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from db import get_engine  # noqa: E402


def main():
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1")).fetchone()
    print("DB OK:", result)


if __name__ == "__main__":
    main()
