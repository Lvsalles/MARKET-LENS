import pandas as pd
import numpy as np
from sqlalchemy import text
from backend.etl import get_engine

class MarketReports:
    def __init__(self):
        self.engine = get_engine()

    def list_all_reports(self):
        """Lists existing reports for the sidebar."""
        query = text("SELECT import_id, report_name, snapshot_date FROM public.stg_mls_imports ORDER BY imported_at DESC")
        with self.engine.connect() as conn:
            return pd.read_sql(query, conn)

    def load_report_data(self, import_id, asset_class):
        """Loads data isolated by Report ID AND Asset Class (Properties, Land, Rental)."""
        # Map the UI names to the database asset_class values
        mapping = {
            "Properties": "Properties",
            "Land": "Land",
            "Rental": "Rental"
        }
        db_class = mapping.get(asset_class, "Properties")
        
        query = text("SELECT * FROM public.stg_mls_classified WHERE import_id = :id AND asset_class = :cls")
        with self.engine.connect() as conn:
            return pd.read_sql(query, conn, params={"id": import_id, "cls": db_class})

    def get_inventory_overview(self, df):
        if df.empty: return pd.DataFrame()
        return df.groupby('zip').agg(
            Listings=('status_group', lambda x: (x == 'listing').sum()),
            Pendings=('status_group', lambda x: (x == 'pending').sum()),
            Sold=('status_group', lambda x: (x == 'closed').sum()),
            Avg_Price=('list_price', 'mean'),
            Avg_Size=('heated_area', 'mean')
        ).reset_index().rename(columns={'zip': 'ZIP CODE'})

    def get_comparison_matrix(self, df):
        """Logic for the Compare Tab."""
        if df.empty: return pd.DataFrame()
        return df.groupby('zip').agg(
            Avg_Price_Sqft=('list_price', lambda x: (x / df.loc[x.index, 'heated_area']).mean()),
            Median_Days=('adom', 'median'),
            Inventory_Count=('ml_number', 'count')
        ).reset_index()
