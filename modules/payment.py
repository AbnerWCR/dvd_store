import logging

import numpy
from numpy import NaN

from modules.base_module import IBaseModule
from infra.db_connection import get_connection
from sqlalchemy import text
import pandas as pd
from datetime import datetime
from models.fact.fact_payment import FactPayment


class Payment:
    INVALID_VALUE = -1

    @classmethod
    def load_stg_s(cls):
        engine, session_context = get_connection()

        df: pd.DataFrame = None
        with session_context() as session:
            try:
                result = session.execute(text(f"""
                                                select
                                                    payment.payment_id,
                                                    payment_date,
                                                    payment.customer_id,
                                                    staff.first_name || ' ' || staff.last_name as staff_full_name,
                                                    payment.amount,
                                                    rental.customer_id as rental_customer_id,
                                                    staff2.first_name || ' ' || staff2.last_name as rental_staff_name,
                                                    rental.rental_date,
                                                    rental.return_date
                                                from public.payment
                                                left join public.staff on payment.staff_id = staff.staff_id
                                                left join public.rental on payment.rental_id = rental.rental_id
                                                left join public.staff staff2 on rental.staff_id = staff2.staff_id"""))
                df = pd.DataFrame(result.fetchall())
                df.columns = result.keys()

                df.rename(columns={
                    'payment_id': 'payment_bk',
                    'customer_id': 'customer_bk',
                    'rental_customer_id': 'rental_customer_bk',
                }, errors='raise', inplace=True)

                df['payment_bk'] = df['payment_bk'].astype('Int64')
                df['customer_bk'] = df['customer_bk'].astype('Int64')
                df['rental_customer_bk'] = df['rental_customer_bk'].astype('Int64')
                df['amount'] = df['amount'].astype(float)
                df['payment_date'] = pd.to_datetime(df['payment_date'])
                df['rental_date'] = pd.to_datetime(df['rental_date'])
                df['return_date'] = pd.to_datetime(df['return_date'])

                df.to_sql('s_payment', schema='stg', con=engine, index=False, if_exists='replace')
            except Exception as ex:
                logging.error(ex)
                return None
        return df

    @classmethod
    def load_fact_payment(cls):
        engine, session_context = get_connection()

        df: pd.DataFrame = None
        with session_context() as session:
            try:
                result_dim_time = session.execute(text("select * from dim.time"))
                df_time = pd.DataFrame(result_dim_time)
                df_time.columns = result_dim_time.keys()
                df_time.rename(columns={
                    'index': 'sk_time',
                }, errors='raise', inplace=True)
                df_time['Date'] = df_time['Date'].apply(lambda x: x.date())

                result = session.execute(text("""select
                                                    pay.payment_bk as bk,
                                                    pay.payment_date,
                                                    cust.sk as customer_sk,
                                                    pay.staff_full_name as staff,
                                                    pay.amount,
                                                    pay.rental_staff_name as rental_staff,
                                                    pay.rental_date,
                                                    pay.return_date
                                                from
                                                    stg.s_payment pay
                                                    left join dim.customer cust on pay.customer_bk = cust.bk
                                                """))
                df = pd.DataFrame(result.fetchall())
                df.columns = result.keys()

                df['sk'] = pd.to_datetime(df['payment_date']).apply(lambda x: x.strftime("%Y%m%d"))
                df['sk'] = df['sk'] + df['bk'].astype(str)
                df['sk'] = df['sk'].astype('Int64')
                init_sk = int(df['sk'].min())
                session.query(FactPayment).filter(FactPayment.sk >= init_sk).delete()
                session.commit()

                df['customer_sk'] = df['customer_sk'].astype('Int64')
                df['customer_sk'] = df['customer_sk'].fillna(-1)

                df['payment_date'] = df['payment_date'].apply(lambda x: x.date())
                df['rental_date'] = df['rental_date'].apply(lambda x: x.date())
                df['return_date'] = df['return_date'].apply(lambda x: x.date())

                df_result = df.merge(df_time,
                                     left_on='payment_date',
                                     right_on='Date',
                                     how='left',
                                     suffixes=('', '_r_pay')).merge(df_time,
                                                                    left_on='rental_date',
                                                                    right_on='Date',
                                                                    how='left',
                                                                    suffixes=('', '_r_rental')).merge(df_time,
                                                                                                      left_on='return_date',
                                                                                                      right_on='Date',
                                                                                                      how='left',
                                                                                                      suffixes=(
                                                                                                      '', '_r_return'))

                df_result = df_result[['bk', 'payment_date', 'customer_sk', 'staff', 'amount', 'rental_staff',
                                       'rental_date', 'return_date', 'sk', 'sk_time', 'sk_time_r_rental',
                                       'sk_time_r_return']]

                df_result.rename(columns={
                    'sk_time': 'sk_payment_date',
                    'sk_time_r_rental': 'sk_rental_date',
                    'sk_time_r_return': 'sk_return_date'
                }, errors='raise', inplace=True)

                df_result.to_sql('payment', con=engine, schema='fact', index=False, if_exists='append')

            except Exception as ex:
                logging.error(ex)
                return None
        return df
