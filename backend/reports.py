import pandas as pd
from backend.etl import get_engine

class MarketReports:
    def __init__(self):
        self.engine = get_engine()

    def list_all_reports(self):
        """Returns a list of all saved reports for the sidebar selector."""
        query = "SELECT import_id, report_name, snapshot_date FROM public.stg_mls_imports ORDER BY imported_at DESC"
        with self.engine.connect() as conn:
            return pd.read_sql(query, conn)

    def load_report_data(self, import_id):
        """Loads data ONLY for the selected report."""
        query = "SELECT * FROM public.stg_mls_classified WHERE import_id = :id"
        with self.engine.connect() as conn:
            return pd.read_sql(pd.text(query), conn, params={"id": import_id})

    def get_inventory_overview(self, df):
        if df.empty: return pd.DataFrame()
        return df.groupby('zip').agg(
            listings=('status_group', lambda x: (x == 'listing').sum()),
            pendings=('status_group', lambda x: (x == 'pending').sum()),
            sold=('status_group', lambda x: (x == 'closed').sum()),
            avg_price=('list_price', 'mean')
        ).reset_index()

    # ... Include other analysis functions (get_size_analysis, etc.) 
    # but ensure they work on the filtered 'df' passed to them.
