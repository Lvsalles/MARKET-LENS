import pandas as pd
import numpy as np
from sqlalchemy import text
from backend.etl import get_engine

class MarketReports:
    def __init__(self):
        self.engine = get_engine()

    def list_all_reports(self):
        query = text("SELECT import_id, report_name, snapshot_date FROM public.stg_mls_imports ORDER BY imported_at DESC")
        with self.engine.connect() as conn:
            return pd.read_sql(query, conn)

    def load_report_data(self, import_id, asset_class):
        # Maps UI names to Database values
        mapping = {"Properties": "Properties", "Land": "Land", "Rental": "Rental"}
        db_cls = mapping.get(asset_class, "Properties")
        
        query = text("SELECT * FROM public.stg_mls_classified WHERE import_id = :id AND asset_class = :cls")
        with self.engine.connect() as conn:
            return pd.read_sql(query, conn, params={"id": import_id, "cls": db_cls})

    def get_inventory_overview(self, df):
        if df.empty: return pd.DataFrame()
        return df.groupby('zip').agg(
            Listings=('ml_number', 'count'),
            Avg_Price=('list_price', 'mean'),
            Avg_Size=('heated_area', 'mean')
        ).reset_index().rename(columns={'zip': 'ZIP CODE'})
