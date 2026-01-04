"""
Normalization helpers — Market Lens (Cloud-first)

Este módulo NÃO contém regras de negócio.
Serve apenas para conversões técnicas:
- strings
- números
- datas
"""

from datetime import date, datetime
from typing import Any, Optional

import pandas as pd


def clean_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    if s.lower() in {"", "nan", "none", "null"}:
        return None
    return s


def to_numeric(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)) and not pd.isna(value):
        return float(value)

    s = clean_string(value)
    if not s:
        return None

    s = s.replace("$", "").replace(",", "")
    try:
        return float(s)
    except Exception:
        return None


def to_integer(value: Any) -> Optional[int]:
    n = to_numeric(value)
    if n is None:
        return None
    return int(n)


def to_date(value: Any) -> Optional[date]:
    if value is None:
        return None

    if isinstance(value, date) and not isinstance(value, datetime):
        return value

    if isinstance(value, (datetime, pd.Timestamp)):
        return value.date()

    s = clean_string(value)
    if not s:
        return None

    parsed = pd.to_datetime(s, errors="coerce")
    if pd.isna(parsed):
        return None

    return parsed.date()
