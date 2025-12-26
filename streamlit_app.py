# streamlit_app.py
import streamlit as st
import pandas as pd

from db import get_db_conn

st.set_page_config(page_title="MARKET LENS — Dashboard", layout="wide")

st.title("MARKET LENS — Dashboard")
st.caption("Market analytics with correct methodologies: weighted averages + simple averages + medians.")


# -------------------------
# DB Helpers
# -------------------------
@st.cache_data(ttl=60)
def fetch_df(sql: str, params=None) -> pd.DataFrame:
    conn = get_db_conn()
    try:
        df = pd.read_sql_query(sql, conn, params=params)
        return df
    finally:
        conn.close()


def metric_card(label: str, value, help_text: str = ""):
    col = st.container()
    col.metric(label, value)
    if help_text:
        col.caption(help_text)


def fmt_money(x):
    if x is None or pd.isna(x):
        return "—"
    return f"${float(x):,.0f}"


def fmt_money2(x):
    if x is None or pd.isna(x):
        return "—"
    return f"${float(x):,.2f}"


def fmt_num(x, digits=0):
    if x is None or pd.isna(x):
        return "—"
    return f"{float(x):,.{digits}f}"


# -------------------------
# Sidebar Navigation
# -------------------------
page = st.sidebar.radio(
    "Navigation",
    ["Overview", "Residential ZIP Snapshot", "Land ZIP Snapshot"],
    index=0
)


# -------------------------
# OVERVIEW PAGE
# -------------------------
if page == "Overview":
    st.subheader("Market Overview (Residential)")

    res = fetch_df("select * from public.v_res_market_overview;")
    if res.empty:
        st.warning("No residential data found.")
        st.stop()

    r = res.iloc[0]

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        metric_card("Homes", int(r["homes"]), "Total rows in residential_listings.")
    with c2:
        metric_card("Avg Price (simple)", fmt_money(r["avg_price_simple"]), "avg(price). Each home weights equally.")
    with c3:
        metric_card("Median Price", fmt_money(r["median_price"]), "50th percentile of price.")
    with c4:
        metric_card("$/SqFt (weighted)", fmt_money2(r["avg_price_per_sqft_weighted"]), "sum(price)/sum(sqft). Best practice.")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        metric_card("Avg SqFt (simple)", fmt_num(r["avg_sqft_simple"], 0), "avg(sqft).")
    with c2:
        metric_card("Median SqFt", fmt_num(r["median_sqft"], 0), "50th percentile of sqft.")
    with c3:
        metric_card("Avg Beds (simple)", fmt_num(r["avg_beds_simple"], 2), "avg(beds).")
    with c4:
        metric_card("Avg Baths (simple)", fmt_num(r["avg_baths_simple"], 2), "avg(baths).")

    st.divider()

    st.subheader("Market Overview (Land)")
    land = fetch_df("select * from public.v_land_market_overview;")
    if land.empty:
        st.warning("No land data found.")
        st.stop()

    l = land.iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        metric_card("Lots", int(l["lots"]), "Total rows in land_listings.")
    with c2:
        metric_card("Avg Lot Price (simple)", fmt_money(l["avg_price_simple"]), "avg(price).")
    with c3:
        metric_card("Median Lot Price", fmt_money(l["median_price"]), "50th percentile of price.")
    with c4:
        metric_card("$/Acre (weighted)", fmt_money2(l["avg_price_per_acre_weighted"]), "sum(price)/sum(acreage). Best practice.")

    c1, c2 = st.columns(2)
    with c1:
        metric_card("Avg Acreage (simple)", fmt_num(l["avg_acreage_simple"], 2), "avg(acreage).")


# -------------------------
# RESIDENTIAL ZIP SNAPSHOT
# -------------------------
elif page == "Residential ZIP Snapshot":
    st.subheader("Residential ZIP Snapshot")

    zips_df = fetch_df("select distinct zip from public.residential_listings where zip is not null order by zip;")
    zip_list = zips_df["zip"].dropna().astype(str).tolist()

    selected = st.multiselect("Filter ZIPs", options=zip_list, default=zip_list[:10] if len(zip_list) >= 10 else zip_list)

    if selected:
        placeholders = ",".join(["%s"] * len(selected))
        df = fetch_df(
            f"""
            select *
            from public.v_res_zip_snapshot
            where zip in ({placeholders})
            order by homes desc;
            """,
            params=selected
        )
    else:
        df = fetch_df("select * from public.v_res_zip_snapshot order by homes desc;")

    if df.empty:
        st.warning("No data to show.")
        st.stop()

    # Display
    df_show = df.copy()
    df_show["avg_price_simple"] = df_show["avg_price_simple"].apply(fmt_money)
    df_show["median_price"] = df_show["median_price"].apply(fmt_money)
    df_show["avg_price_per_sqft_weighted"] = df_show["avg_price_per_sqft_weighted"].apply(fmt_money2)
    df_show["avg_sqft_simple"] = df_show["avg_sqft_simple"].apply(lambda x: fmt_num(x, 0))

    st.dataframe(
        df_show.rename(columns={
            "zip": "ZIP",
            "homes": "Homes",
            "avg_price_simple": "Avg Price (simple)",
            "median_price": "Median Price",
            "avg_sqft_simple": "Avg SqFt (simple)",
            "avg_beds_simple": "Avg Beds",
            "avg_baths_simple": "Avg Baths",
            "avg_price_per_sqft_weighted": "$/SqFt (weighted)",
        }),
        use_container_width=True
    )

    st.caption("Note: $/SqFt uses sum(price)/sum(sqft) to avoid distortion from small homes dominating the average.")


# -------------------------
# LAND ZIP SNAPSHOT
# -------------------------
else:
    st.subheader("Land ZIP Snapshot")

    zips_df = fetch_df("select distinct zip from public.land_listings where zip is not null order by zip;")
    zip_list = zips_df["zip"].dropna().astype(str).tolist()

    selected = st.multiselect("Filter ZIPs", options=zip_list, default=zip_list[:10] if len(zip_list) >= 10 else zip_list)

    if selected:
        placeholders = ",".join(["%s"] * len(selected))
        df = fetch_df(
            f"""
            select *
            from public.v_land_zip_snapshot
            where zip in ({placeholders})
            order by lots desc;
            """,
            params=selected
        )
    else:
        df = fetch_df("select * from public.v_land_zip_snapshot order by lots desc;")

    if df.empty:
        st.warning("No data to show.")
        st.stop()

    df_show = df.copy()
    df_show["avg_price_simple"] = df_show["avg_price_simple"].apply(fmt_money)
    df_show["median_price"] = df_show["median_price"].apply(fmt_money)
    df_show["avg_price_per_acre_weighted"] = df_show["avg_price_per_acre_weighted"].apply(fmt_money2)
    df_show["avg_acreage_simple"] = df_show["avg_acreage_simple"].apply(lambda x: fmt_num(x, 2))

    st.dataframe(
        df_show.rename(columns={
            "zip": "ZIP",
            "lots": "Lots",
            "avg_price_simple": "Avg Price (simple)",
            "median_price": "Median Price",
            "avg_acreage_simple": "Avg Acreage (simple)",
            "avg_price_per_acre_weighted": "$/Acre (weighted)",
        }),
        use_container_width=True
    )

    st.caption("Note: $/Acre uses sum(price)/sum(acreage).")
