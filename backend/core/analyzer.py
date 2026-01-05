import pandas as pd
from sqlalchemy import text
from backend.etl import get_engine

class MarketAnalyzer:
    def __init__(self):
        self.engine = get_engine()

    def find_undervalued_deals(self, threshold=0.90):
        """Encontra imóveis 10% ou mais abaixo da média de preço/sqft do bairro."""
        query = "SELECT * FROM public.stg_mls_classified"
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)
        
        if df.empty: return pd.DataFrame()

        # Calcula média por ZIP
        df['price_sqft'] = df['list_price'] / df['heated_area']
        zip_averages = df.groupby('zip')['price_sqft'].mean().reset_index()
        zip_averages.columns = ['zip', 'avg_price_sqft_zip']

        # Cruza dados
        df = df.merge(zip_averages, on='zip')
        df['deal_score'] = df['price_sqft'] / df['avg_price_sqft_zip']

        # Filtra apenas o que está em estoque (listings) e abaixo do preço
        deals = df[(df['status_group'] == 'listing') & (df['deal_score'] <= threshold)]
        return deals.sort_values('deal_score')
