import pandas as pd
from sqlalchemy import text
from backend.etl import get_engine

class MarketReports:
    def __init__(self):
        try:
            self.engine = get_engine()
        except:
            self.engine = None

    def list_all_reports(self):
        if not self.engine: return pd.DataFrame()
        query = text("SELECT import_id, report_name, snapshot_date FROM public.stg_mls_imports ORDER BY imported_at DESC")
        with self.engine.connect() as conn:
            return pd.read_sql(query, conn)

    def load_report_data(self, import_id, category):
        if not self.engine: return pd.DataFrame()
        query = text("SELECT * FROM public.stg_mls_classified WHERE import_id = :id AND asset_class = :cls")
        with self.engine.connect() as conn:
            return pd.read_sql(query, conn, params={"id": import_id, "cls": category})

    def get_inventory_overview(self, df):
        if df.empty: return pd.DataFrame()
        return df.groupby('zip').agg(
            Listings=('ml_number', 'count'),
            Avg_Price=('list_price', 'mean'),
            Avg_ADOM=('adom', 'mean')
        ).reset_index().rename(columns={'zip': 'ZIP CODE'})
