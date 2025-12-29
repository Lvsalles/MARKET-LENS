from __future__ import annotations
import pandas as pd
import numpy as np


def _to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def weighted_mean(values: pd.Series, weights: pd.Series) -> float | None:
    v = _to_num(values)
    w = _to_num(weights).fillna(0)
    mask = v.notna() & (w > 0)
    if mask.sum() == 0:
        return None
    return float((v[mask] * w[mask]).sum() / w[mask].sum())


def arithmetic_mean(values: pd.Series) -> float | None:
    v = _to_num(values)
    if v.dropna().empty:
        return None
    return float(v.mean())


def compute_metrics_for_group(df: pd.DataFrame) -> dict:
    """
    Regras Market Lens:
      - média ponderada: size (sqft), price, ADOM
      - média aritmética: beds, baths, garage
    Observação:
      Dentro de um grupo, cada imóvel pesa 1 (ponderação por registro).
      Para “overall via zip”, pondera pelo record_count do zip.
    """
    n = int(len(df))
    w = pd.Series(np.ones(n), index=df.index)

    out = {
        "record_count": n,
        "avg_price": weighted_mean(df.get("price", pd.Series(dtype=float)), w),
        "avg_sqft": weighted_mean(df.get("sqft", pd.Series(dtype=float)), w),
        "avg_adom": weighted_mean(df.get("adom", pd.Series(dtype=float)), w),
        "avg_ppsqft": weighted_mean(df.get("ppsqft", pd.Series(dtype=float)), w),
        "avg_beds": arithmetic_mean(df.get("beds", pd.Series(dtype=float))),
        "avg_baths": arithmetic_mean(df.get("baths", pd.Series(dtype=float))),
        "avg_garage": arithmetic_mean(df.get("garage", pd.Series(dtype=float))),
    }
    return out


def rollup_overall_from_zip_table(zip_table: pd.DataFrame) -> dict:
    """
    Recebe uma tabela por ZIP com:
      record_count, avg_price, avg_sqft, avg_adom, avg_ppsqft, avg_beds, avg_baths, avg_garage
    Overall = ponderar pelo record_count do ZIP (proporcionalidade).
    """
    if zip_table is None or zip_table.empty:
        return {}

    w = _to_num(zip_table["record_count"]).fillna(0)
    if float(w.sum()) <= 0:
        return {}

    return {
        "record_count": int(w.sum()),
        "avg_price": weighted_mean(zip_table["avg_price"], w),
        "avg_sqft": weighted_mean(zip_table["avg_sqft"], w),
        "avg_adom": weighted_mean(zip_table["avg_adom"], w),
        "avg_ppsqft": weighted_mean(zip_table["avg_ppsqft"], w),
        "avg_beds": weighted_mean(zip_table["avg_beds"], w),
        "avg_baths": weighted_mean(zip_table["avg_baths"], w),
        "avg_garage": weighted_mean(zip_table["avg_garage"], w),
    }
