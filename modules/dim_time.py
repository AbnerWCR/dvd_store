import pandas as pd
from infra.db_connection import get_connection
from datetime import datetime


def create_dim_time():
    try:
        date_range = {'Date': pd.date_range(start='2021-01-01', end='2023-12-31')}
        df_date_range = pd.DataFrame(date_range)

        df_date_range['year'] = df_date_range['Date'].dt.year
        df_date_range['quarter'] = df_date_range['Date'].dt.quarter
        df_date_range['month'] = df_date_range['Date'].dt.month
        df_date_range['month_name'] = df_date_range['Date'].dt.month_name()
        df_date_range['day'] = df_date_range['Date'].dt.day
        df_date_range['week'] = df_date_range['Date'].dt.isocalendar().week

        engine, session_context = get_connection()
        index_max = df_date_range.index.max()
        df_time = pd.DataFrame(df_date_range, index=pd.RangeIndex(start=1, stop=int(index_max), step=1))

        df_time.to_sql('time', con=engine, schema='dim', if_exists='replace')
        return df_time
    except Exception as ex:
        print(ex)
        return None
