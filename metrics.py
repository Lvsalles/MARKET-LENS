import pandas as pd
from sqlalchemy import text


# =========================
# Read
# =========================
def read_stg(engine, project_id: str, categories: list[str] | None = None) -> pd.DataFrame:
    if categories:
        q = text("""
            SELECT *
            FROM stg_mls
            WHERE project_id = :project_id
              AND category = ANY(:categories)
        """)
        with engine.begin() as conn:
            return pd.read_sql(q, conn, params={"project_id": project_id, "categories": categories})

    q = text("""
        SELECT *
        FROM stg_mls
        WHERE project_id = :project_id
    """)
    with engine.begin() as conn:
        return pd.read_sql(q, conn, params={"project_id": project_id})


def table_row_counts(engine, project_id: str) -> pd.DataFrame:
    q = text("""
        SELECT category, COUNT(*)::int AS rows
        FROM stg_mls
        WHERE project_id = :project_id
        GROUP BY category
        ORDER BY category
    """)
    with engine.begin() as conn:
        rows = conn.execute(q, {"project_id": project_id}).fetchall()
    return pd.DataFrame(rows, columns=["category", "rows"])


# =========================
# Weighted math (core)
# =========================
def _to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def weighted_mean(values: pd.Series, weights: pd.Series) -> float | None:
    v = _to_num(values)
    w = _to_num(weights)

    mask = v.notna() & w.notna() & (w > 0)
    v = v[mask]
    w = w[mask]

    if len(v) == 0:
        return None

    return float((v * w).sum() / w.sum())


def weighted_quantile(values: pd.Series, weights: pd.Series, q: float) -> float | None:
    """
    Quantil ponderado (aproximação correta):
    - ordena por value
    - acumula pesos
    - encontra ponto onde cum_weight / total_weight >= q
    """
    v = _to_num(values)
    w = _to_num(weights)

    mask = v.notna() & w.notna() & (w > 0)
    v = v[mask]
    w = w[mask]

    if len(v) == 0:
        return None

    order = v.argsort()
    v_sorted = v.iloc[order].reset_index(drop=True)
    w_sorted = w.iloc[order].reset_index(drop=True)

    cum_w = w_sorted.cumsum()
    total_w = w_sorted.sum()
    if total_w <= 0:
        return None

    target = q * total_w
    idx = cum_w.searchsorted(target, side="left")
    idx = min(int(idx), len(v_sorted) - 1)

    return float(v_sorted.iloc[idx])


def weighted_quartiles(values: pd.Series, weights: pd.Series) -> dict:
    return {
        "p25": weighted_quantile(values, weights, 0.25),
        "median": weighted_quantile(values, weights, 0.50),
        "p75": weighted_quantile(values, weights, 0.75),
        "wavg": weighted_mean(values, weights),
    }


def get_weights(df: pd.DataFrame, weight_col: str = "sqft") -> pd.Series:
    """
    Peso padrão: sqft. Se sqft não existir ou tiver NaN, peso vira 1.
    """
    if weight_col in df.columns:
        w = _to_num(df[weight_col]).fillna(1)
        w = w.where(w > 0, 1)
        return w
    return pd.Series([1] * len(df), index=df.index, dtype="float64")


# =========================
# Diagnostics
# =========================
def missingness_report(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["column", "missing_pct", "missing_count", "total_rows"])

    total = len(df)
    missing_count = df.isna().sum()
    missing_pct = (missing_count / total * 100).round(2)

    out = pd.DataFrame({
        "column": missing_count.index,
        "missing_pct": missing_pct.values,
        "missing_count": missing_count.values,
        "total_rows": total
    }).sort_values(["missing_pct", "missing_count"], ascending=False)

    return out


def duplicates_report(df: pd.DataFrame) -> pd.DataFrame:
    cols = []
    if "mls_id" in df.columns:
        cols.append("mls_id")
    if "address" in df.columns:
        cols.append("address")

    if len(cols) < 1:
        return pd.DataFrame(columns=["key", "count"])

    key_cols = cols[:2]
    d = (
        df.dropna(subset=key_cols)
          .groupby(key_cols)
          .size()
          .reset_index(name="count")
          .query("count > 1")
          .sort_values("count", ascending=False)
    )

    if len(key_cols) == 2:
        d["key"] = d[key_cols[0]].astype(str) + " | " + d[key_cols[1]].astype(str)
        d = d[["key", "count"]]
    else:
        d["key"] = d[key_cols[0]].astype(str)
        d = d[["key", "count"]]

    return d.head(200)


def outliers_report(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["rule", "count"])

    rules = []

    def add_rule(name: str, mask):
        rules.append({"rule": name, "count": int(mask.sum())})

    if "sqft" in df.columns:
        add_rule("sqft <= 0", _to_num(df["sqft"]).fillna(0) <= 0)

    if "list_price" in df.columns:
        add_rule("list_price <= 0", _to_num(df["list_price"]).fillna(0) <= 0)

    if "sold_price" in df.columns:
        add_rule("sold_price <= 0", _to_num(df["sold_price"]).fillna(0) <= 0)

    if "adom" in df.columns:
        add_rule("adom < 0", _to_num(df["adom"]).fillna(0) < 0)

    if "dom" in df.columns:
        add_rule("dom < 0", _to_num(df["dom"]).fillna(0) < 0)

    if "sp_lp" in df.columns:
        sp = _to_num(df["sp_lp"])
        add_rule("sp_lp < 0.70", sp.fillna(1) < 0.70)
        add_rule("sp_lp > 1.10", sp.fillna(1) > 1.10)

    return pd.DataFrame(rules).sort_values("count", ascending=False)


# =========================
# Overview (weighted where requested)
# =========================
def investor_grade_overview(df: pd.DataFrame) -> pd.DataFrame:
    """
    Você pediu:
      - PONDERADA: preço, $/sqft, ADOM, tamanho, beds, baths
      - ARITMÉTICA: demais (não aplicável aqui nos cards)
    Peso padrão: sqft (fallback=1)
    """
    if df.empty:
        return pd.DataFrame(columns=["metric", "p25", "median", "p75", "weighted_avg", "notes"])

    w = get_weights(df, "sqft")
    rows = []

    def add(metric: str, col: str, notes: str):
        if col not in df.columns:
            return
        q = weighted_quartiles(df[col], w)
        rows.append({
            "metric": metric,
            "p25": q["p25"],
            "median": q["median"],
            "p75": q["p75"],
            "weighted_avg": q["wavg"],
            "notes": notes
        })

    # SOLD (preferencial)
    add("Sold Price", "sold_price", "Weighted by sqft")
    add("$/Sqft (ppsqft)", "ppsqft", "Weighted by sqft")
    add("ADOM", "adom", "Weighted by sqft")
    add("Sqft", "sqft", "Weighted by sqft")
    add("Beds", "beds", "Weighted by sqft (as requested)")
    add("Baths", "baths", "Weighted by sqft (as requested)")
    add("SP/LP", "sp_lp", "Weighted by sqft")

    # Listings (se sold_price não existir, você ainda vê algo)
    add("List Price", "list_price", "Weighted by sqft")

    return pd.DataFrame(rows)


def monthly_snapshot_weighted(df: pd.DataFrame, value_col: str, weight_col: str = "sqft") -> pd.DataFrame:
    """
    Snapshot mensal com:
      - weighted_avg por mês (peso= sq ft)
      - MoM e YoY em cima do weighted_avg
    """
    if df.empty or "month_key" not in df.columns or value_col not in df.columns:
        return pd.DataFrame(columns=["month", "weighted_avg", "mom_pct", "yoy_pct", "n", "sum_weights"])

    temp = df.copy()
    temp["month"] = pd.to_datetime(temp["month_key"], errors="coerce")
    temp[value_col] = _to_num(temp[value_col])

    if weight_col in temp.columns:
        temp[weight_col] = _to_num(temp[weight_col]).fillna(1)
        temp[weight_col] = temp[weight_col].where(temp[weight_col] > 0, 1)
    else:
        temp[weight_col] = 1.0

    temp = temp.dropna(subset=["month", value_col])
    if temp.empty:
        return pd.DataFrame(columns=["month", "weighted_avg", "mom_pct", "yoy_pct", "n", "sum_weights"])

    temp["period"] = temp["month"].dt.to_period("M")

    def agg(group: pd.DataFrame):
        wavg = weighted_mean(group[value_col], group[weight_col])
        return pd.Series({
            "weighted_avg": wavg,
            "n": int(len(group)),
            "sum_weights": float(_to_num(group[weight_col]).sum())
        })

    g = temp.groupby("period").apply(agg).reset_index()
    g["month"] = g["period"].astype(str)
    g = g.drop(columns=["period"]).sort_values("month")

    g["mom_pct"] = pd.to_numeric(g["weighted_avg"], errors="coerce").pct_change() * 100
    g["yoy_pct"] = pd.to_numeric(g["weighted_avg"], errors="coerce").pct_change(12) * 100

    return g
