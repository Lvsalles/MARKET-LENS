import pandas as pd
import numpy as np
from sqlalchemy import text
from backend.etl import get_engine

class MarketReports:
    def __init__(self):
        self.engine = get_engine()

    def load_data(self):
        query = "SELECT * FROM public.stg_mls_classified"
        return pd.read_sql(query, self.engine)

    def get_size_analysis(self, df):
        """Tabela Estilo Screenshot 11: House Size vs Zip Codes"""
        # Criar faixas de tamanho (bins de 50 em 50)
        df['size_bucket'] = (df['heated_area'] // 50) * 50
        
        # Pivot table para cruzar Tamanho com ZIP Codes
        report = df.groupby('size_bucket').agg(
            casas_vendidas=('ml_number', 'count'),
            valor_medio=('close_price', 'mean'),
            avg_sqft_price=('list_price', lambda x: (x / df.loc[x.index, 'heated_area']).mean())
        ).reset_index()
        
        # Adicionar colunas de ZIP codes dinamicamente
        zip_pivot = df.pivot_table(index='size_bucket', columns='zip', values='ml_number', aggfunc='count', fill_value=0)
        
        return pd.concat([report.set_index('size_bucket'), zip_pivot], axis=1).reset_index()

    def get_year_analysis(self, df):
        """Tabela Estilo Screenshot 12: Building Year vs Price Range"""
        # Definir faixas de preço
        bins = [0, 300000, 350000, 400000, 450000, 500000, float('inf')]
        labels = ['0-300K', '300-350K', '350-400K', '400-450K', '450-500K', '500K+']
        df['price_range'] = pd.cut(df['list_price'], bins=bins, labels=labels)
        
        report = df.groupby('year_built').agg(
            casas_vendidas=('ml_number', 'count'),
            valor_medio=('list_price', 'mean'),
            tamanho_medio=('heated_area', 'mean'),
            adom_medio=('adom', 'mean')
        )
        
        price_pivot = df.pivot_table(index='year_built', columns='price_range', values='ml_number', aggfunc='count', fill_value=0)
        return pd.concat([report, price_pivot], axis=1).reset_index()

    def get_inventory_overview(self, df):
        """Tabela Estilo Screenshot 15: Overview by ZIP"""
        return df.groupby('zip').agg(
            listings=('status_group', lambda x: (x == 'listing').sum()),
            pendings=('status_group', lambda x: (x == 'pending').sum()),
            sold=('status_group', lambda x: (x == 'closed').sum()),
            avg_price=('list_price', 'mean'),
            avg_size=('heated_area', 'mean'),
            avg_beds=('beds', 'mean'),
            avg_baths=('full_baths', 'mean')
        ).reset_index()
