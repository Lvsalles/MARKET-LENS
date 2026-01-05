import pandas as pd
import numpy as np
from sqlalchemy import text
from backend.etl import get_engine

class MarketReports:
    def __init__(self):
        self.engine = get_engine()

    def load_data(self):
        """Carrega todos os dados classificados do banco."""
        query = "SELECT * FROM public.stg_mls_classified"
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)
        return df

    def get_size_analysis(self, df):
        """Recria a lógica do seu Screenshot 11: House Size vs Zip Codes."""
        if df.empty: return pd.DataFrame()
        
        # Criar faixas de tamanho de 50 em 50 sqft
        df['size_bucket'] = (df['heated_area'] // 50) * 50
        
        # Agrupar métricas principais
        report = df.groupby('size_bucket').agg(
            casas_vendidas=('ml_number', 'count'),
            valor_medio=('list_price', 'mean'),
            price_per_sqft=('list_price', lambda x: (x / df.loc[x.index, 'heated_area']).mean())
        )
        
        # Criar colunas dinâmicas para cada ZIP Code encontrado
        zip_pivot = df.pivot_table(
            index='size_bucket', 
            columns='zip', 
            values='ml_number', 
            aggfunc='count', 
            fill_value=0
        )
        
        return pd.concat([report, zip_pivot], axis=1).reset_index().rename(columns={'size_bucket': 'HOUSE SIZE'})

    def get_year_analysis(self, df):
        """Recria a lógica do seu Screenshot 12: Building Year vs Price Range."""
        if df.empty: return pd.DataFrame()

        # Definir faixas de preço conforme seu print
        bins = [0, 300000, 350000, 400000, 450000, 500000, float('inf')]
        labels = ['0-300K', '300-350K', '350-400K', '400-450K', '450-500K', '500K+']
        df['price_range'] = pd.cut(df['list_price'], bins=bins, labels=labels)
        
        # Agrupar por ano de construção
        report = df.groupby('year_built').agg(
            casas_vendidas=('ml_number', 'count'),
            valor_medio=('list_price', 'mean'),
            tamanho_medio=('heated_area', 'mean'),
            adom_medio=('adom', 'mean')
        )
        
        # Pivotar faixas de preço
        price_pivot = df.pivot_table(
            index='year_built', 
            columns='price_range', 
            values='ml_number', 
            aggfunc='count', 
            fill_value=0
        )
        
        return pd.concat([report, price_pivot], axis=1).reset_index().rename(columns={'year_built': 'BUILDING YEAR'})

    def get_inventory_overview(self, df):
        """Recria a lógica do seu Screenshot 15: Resumo por ZIP."""
        if df.empty: return pd.DataFrame()
        
        return df.groupby('zip').agg(
            listings=('status_group', lambda x: (x == 'listing').sum()),
            pendings=('status_group', lambda x: (x == 'pending').sum()),
            sold=('status_group', lambda x: (x == 'closed').sum()),
            avg_price=('list_price', 'mean'),
            avg_size=('heated_area', 'mean'),
            avg_beds=('beds', 'mean'),
            avg_baths=('full_baths', 'mean')
        ).reset_index().rename(columns={'zip': 'ZIP CODE'})
