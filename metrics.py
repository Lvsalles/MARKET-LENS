from __future__ import annotations
import pandas as pd
import numpy as np

def safe_numeric(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")

def weighted_mean(values: pd.Series, weights: pd.Series) -> float | None:
    v = safe_numeric(values)
    w = safe_numeric(weights).fillna(0)
    mask = v.notna() & (w > 0)
    if mask.sum() == 0:
        return None
    return float((v[mask] * w[mask]).sum() / w[mask].sum())

def arithmetic_mean(values: pd.Series) -> float | None:
    v = safe_numeric(values)
    if v.dropna().empty:
        return None
    return float(v.mean())

def compute_group_metrics(df: pd.DataFrame) -> dict:
    """
    df = linhas de propriedades (por exemplo um ZIP + mês + categoria)
    regra:
      - price, sqft, adom: ponderada (na prática por imóvel => weights=1)
      - beds, baths, garage: aritmética
    """
    n = int(len(df))

    # peso básico por registro (cada imóvel = 1)
    w = pd.Series(np.ones(n), index=df.index)

    price = weighted_mean(df["price"], w)
    sqft = weighted_mean(df["sqft"], w)
    adom = weighted_mean(df["adom"], w)

    # ppsqft: calculado como média ponderada do ppsqft por imóvel
    # (evita distorção de dividir médias)
    ppsqft = weighted_mean(df["ppsqft"], w)

    beds = arithmetic_mean(df["beds"])
    baths = arithmetic_mean(df["baths"])
    garage = arithmetic_mean(df["garage"])

    return {
        "record_count": n,
        "avg_price": price,
        "avg_sqft": sqft,
        "avg_ppsqft": ppsqft,
        "avg_adom": adom,
        "avg_beds": beds,
        "avg_baths": baths,
        "avg_garage": garage,
    }

def rollup_overall_from_zip_facts(zip_facts: pd.DataFrame) -> dict:
    """
    zip_facts deve ter colunas: record_count, avg_price, avg_sqft, avg_ppsqft, avg_adom, avg_beds, avg_baths, avg_garage
    Overall deve ser ponderado pelo volume do ZIP (proporcionalidade).
    """
    if zip_facts.empty:
        return {}

    w = safe_numeric(zip_facts["record_count"]).fillna(0)
    if w.sum() == 0:
        return {}

    out = {
        "record_count": int(w.sum()),
        "avg_price": weighted_mean(zip_facts["avg_price"], w),
        "avg_sqft": weighted_mean(zip_facts["avg_sqft"], w),
        "avg_ppsqft": weighted_mean(zip_facts["avg_ppsqft"], w),
        "avg_adom": weighted_mean(zip_facts["avg_adom"], w),
        # beds/baths/garage: se os ZIPs já têm médias, overall correto = ponderar por count
        "avg_beds": weighted_mean(zip_facts["avg_beds"], w),
        "avg_baths": weighted_mean(zip_facts["avg_baths"], w),
        "avg_garage": weighted_mean(zip_facts["avg_garage"], w),
    }
    return out
