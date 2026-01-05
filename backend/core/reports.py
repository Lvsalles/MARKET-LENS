import pandas as pd
import numpy as np
from backend.etl import get_engine

class MarketReports:
    def __init__(self):
        self.engine = get_engine()

    def load_data(self):
        """Carrega os dados classificados do banco de dados."""
        query = "SELECT * FROM public.stg_mls_classified"
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)
        return df

    def get_inventory_overview(self, df):
        """Resumo por ZIP Code (Tab Resumo)."""
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

    def get_size_analysis(self, df):
        """House Size vs Zip Codes (Tab Tamanho)."""
        if df.empty: return pd.DataFrame()
        df_sold = df[df['status_group'] == 'closed'].copy()
        if df_sold.empty: df_sold = df.copy() # Fallback se não houver vendidos
        
        df_sold['HOUSE SIZE'] = (df_sold['heated_area'] // 50) * 50
        report = df_sold.groupby('HOUSE SIZE').agg(
            CASAS_VENDIDAS=('ml_number', 'count'),
            VALOR_MEDIO=('close_price', 'mean'),
            SQFT_PRICE=('close_price', lambda x: (x / df_sold.loc[x.index, 'heated_area']).mean())
        )
        zip_pivot = df_sold.pivot_table(index='HOUSE SIZE', columns='zip', values='ml_number', aggfunc='count', fill_value=0)
        res = pd.concat([report, zip_pivot], axis=1).reset_index()
        res.rename(columns={'VALOR_MEDIO': 'VALOR MÉDIO', 'SQFT_PRICE': '$/SQFT'}, inplace=True)
        return res

    def get_year_analysis(self, df):
        """Building Year vs Price Range (Tab Ano/Preço)."""
        if df.empty: return pd.DataFrame()
        bins = [0, 300000, 350000, 400000, 450000, 500000, float('inf')]
        labels = ['0-300K', '300-350K', '350-400K', '400-450K', '450-500K', '500K+']
        df['price_range'] = pd.cut(df['list_price'], bins=bins, labels=labels)
        
        report = df.groupby('year_built').agg(
            CASAS_VENDIDAS=('ml_number', 'count'),
            VALOR_MEDIO=('list_price', 'mean'),
            TAMANHO_MEDIO=('heated_area', 'mean'),
            SQFT_PRICE=('list_price', lambda x: (x / df.loc[x.index, 'heated_area']).mean()),
            ADOM=('adom', 'mean')
        )
        price_pivot = df.pivot_table(index='year_built', columns='price_range', values='ml_number', aggfunc='count', fill_value=0)
        res = pd.concat([report, price_pivot], axis=1).reset_index()
        res.rename(columns={'year_built': 'BUILDING YEAR', 'VALOR_MEDIO': 'VALOR MÉDIO', 'TAMANHO_MEDIO': 'TAMANHO MÉDIO', 'SQFT_PRICE': '$/SQFT'}, inplace=True)
        return res

    def get_mom_analysis(self, df):
        """Month over Month Analysis (Tab MoM)."""
        if df.empty: return pd.DataFrame()
        df['close_date'] = pd.to_datetime(df['close_date'])
        df = df.dropna(subset=['close_date'])
        if df.empty: return pd.DataFrame(columns=['STARTING DATE', 'CASAS VENDIDAS'])
        
        df['month'] = df['close_date'].dt.strftime('%b-%y')
        df['month_sort'] = df['close_date'].dt.to_period('M')
        
        report = df.groupby(['month_sort', 'month']).agg(
            CASAS_VENDIDAS=('ml_number', 'count'),
            VALOR_MEDIO=('close_price', 'mean'),
            TAMANHO_MEDIO=('heated_area', 'mean'),
            SQFT_PRICE=('close_price', lambda x: (x / df.loc[x.index, 'heated_area']).mean()),
            ADOM=('adom', 'mean')
        ).reset_index().sort_values('month_sort')
        
        return report.drop(columns='month_sort').rename(columns={'month': 'STARTING DATE', 'VALOR_MEDIO': 'VALOR MÉDIO', 'TAMANHO_MEDIO': 'TAMANHO MÉDIO', 'SQFT_PRICE': '$/SQFT'})
