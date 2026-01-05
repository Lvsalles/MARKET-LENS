import pandas as pd
import numpy as np
from sqlalchemy import text
from backend.etl import get_engine

class MarketReports:
    def __init__(self):
        self.engine = get_engine()

    def list_all_reports(self):
        """Lists all existing isolated reports."""
        query = text("SELECT import_id, report_name, snapshot_date FROM public.stg_mls_imports ORDER BY imported_at DESC")
        with self.engine.connect() as conn:
            return pd.read_sql(query, conn)

    def load_report_data(self, import_id):
        """Loads data filtered by a specific report ID."""
        query = text("SELECT * FROM public.stg_mls_classified WHERE import_id = :id")
        with self.engine.connect() as conn:
            return pd.read_sql(query, conn, params={"id": import_id})

    def get_inventory_overview(self, df):
        if df.empty: return pd.DataFrame()
        return df.groupby('zip').agg(
            listings=('status_group', lambda x: (x == 'listing').sum()),
            pendings=('status_group', lambda x: (x == 'pending').sum()),
            sold=('status_group', lambda x: (x == 'closed').sum()),
            avg_price=('list_price', 'mean'),
            avg_size=('heated_area', 'mean')
        ).reset_index().rename(columns={'zip': 'ZIP CODE'})

    def get_size_analysis(self, df):
        """Size analysis table logic."""
        if df.empty: return pd.DataFrame()
        df['HOUSE SIZE'] = (df['heated_area'].fillna(0) // 50) * 50
        report = df.groupby('HOUSE SIZE').agg(
            SOLD_HOUSES=('ml_number', 'count'),
            AVG_VALUE=('close_price', 'mean'),
            SQFT_PRICE=('close_price', lambda x: (x / df.loc[x.index, 'heated_area']).mean())
        )
        zip_pivot = df.pivot_table(index='HOUSE SIZE', columns='zip', values='ml_number', aggfunc='count', fill_value=0)
        res = pd.concat([report, zip_pivot], axis=1).reset_index()
        res.rename(columns={'AVG_VALUE': 'AVERAGE VALUE', 'SQFT_PRICE': '$/SQFT'}, inplace=True)
        return res

    def get_year_analysis(self, df):
        """Building year analysis logic."""
        if df.empty: return pd.DataFrame()
        bins = [0, 300000, 350000, 400000, 450000, 500000, float('inf')]
        labels = ['0-300K', '300-350K', '350-400K', '400-450K', '450-500K', '500K+']
        df['price_range'] = pd.cut(df['list_price'], bins=bins, labels=labels)
        report = df.groupby('year_built').agg(
            SOLD_HOUSES=('ml_number', 'count'),
            AVG_VALUE=('list_price', 'mean'),
            AVG_SIZE=('heated_area', 'mean'),
            ADOM=('adom', 'mean')
        )
        price_pivot = df.pivot_table(index='year_built', columns='price_range', values='ml_number', aggfunc='count', fill_value=0)
        res = pd.concat([report, price_pivot], axis=1).reset_index()
        res.rename(columns={'year_built': 'BUILDING YEAR', 'AVG_VALUE': 'AVERAGE VALUE', 'AVG_SIZE': 'AVERAGE SIZE'}, inplace=True)
        return res
