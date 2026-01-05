import pandas as pd
from sqlalchemy import text
from backend.etl import get_engine

class MarketReports:
    def __init__(self):
        self.engine = get_engine()

    def list_reports(self):
        query = text("SELECT import_id, report_name, snapshot_date FROM public.stg_mls_imports ORDER BY imported_at DESC")
        with self.engine.connect() as conn:
            return pd.read_sql(query, conn)

    def load_data(self, import_id, category):
        query = text("SELECT * FROM public.stg_mls_classified WHERE import_id = :id AND asset_class = :cls")
        with self.engine.connect() as conn:
            return pd.read_sql(query, conn, params={"id": import_id, "cls": category.lower()})

    def get_summary(self, df):
        if df.empty: return pd.DataFrame()
        return df.groupby('zip').agg(
            Volume=('ml_number', 'count'),
            Avg_Price=('list_price', 'mean'),
            Avg_ADOM=('adom', 'mean')
        ).reset_index().rename(columns={'zip': 'ZIP CODE'})
