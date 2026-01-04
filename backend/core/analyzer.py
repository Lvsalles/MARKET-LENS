import pandas as pd
from sqlalchemy import text
from backend.etl import get_engine

class MarketAnalyzer:
    def __init__(self):
        self.engine = get_engine()

    def get_market_stats(self):
        """Calcula a média de preço por SqFt e por Quarto em cada ZIP Code."""
        query = text("""
            SELECT 
                zip,
                asset_class,
                status_group,
                ROUND(AVG(list_price / NULLIF(heated_area, 0)), 2) as avg_price_sqft,
                ROUND(AVG(list_price), 2) as avg_total_price,
                COUNT(*) as property_count
            FROM public.stg_mls_classified
            WHERE heated_area > 0 AND list_price > 0
            GROUP BY zip, asset_class, status_group
        """)
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)
        return df

    def find_undervalued_deals(self, threshold=0.85):
        """
        Encontra imóveis que estão listados por um valor menor que 
        o 'threshold' (ex: 85%) da média do bairro.
        """
        # 1. Pega os dados estruturados
        with self.engine.connect() as conn:
            listings = pd.read_sql("SELECT * FROM public.stg_mls_classified WHERE status_group = 'listing'", conn)
        
        stats = self.get_market_stats()
        
        # 2. Cruza os dados
        merged = listings.merge(stats, on=['zip', 'asset_class'], suffixes=('', '_market'))
        
        # 3. Calcula o desvio
        merged['price_sqft'] = merged['list_price'] / merged['heated_area']
        merged['deal_score'] = merged['price_sqft'] / merged['avg_price_sqft']
        
        # Filtra oportunidades (abaixo da média)
        deals = merged[merged['deal_score'] <= threshold].sort_values(by='deal_score')
        
        return deals
