import pandas as pd
from sqlalchemy import text
from backend.etl import get_engine

class MarketReports:
    def __init__(self):
        self.engine = get_engine()

    def list_reports(self):
        return pd.read_sql("SELECT import_id, report_name, snapshot_date FROM public.stg_imports_view ORDER BY imported_at DESC", self.engine)

    def load_data(self, import_id, category):
        query = text("SELECT * FROM public.stg_mls_classified WHERE import_id = :id AND asset_class = :cls")
        with self.engine.connect() as conn:
            return pd.read_sql(query, conn, params={"id": import_id, "cls": category})

    def get_summary(self, df):
        if df.empty: return pd.DataFrame()
        return df.groupby('zip').agg(Listings=('ml_number', 'count'), Avg_Price=('list_price', 'mean')).reset_index()
