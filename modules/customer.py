from modules.base_module import IBaseModule
from infra.db_connection import get_connection
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from models.stg.stg_customer import StgCustomer
from models.dim.dim_customer import DimCustomer
from modules.address import Address
from modules.store import Store
import pandas as pd
import logging


class Customer(IBaseModule):

    @classmethod
    def load_stg(cls) -> pd.DataFrame:
        engine, session_context = get_connection()
        df_final = pd.DataFrame()

        with session_context() as session:
            try:
                session.query(StgCustomer).delete()
                session.commit()

                result = session.execute(text(f"""select * from public.customer where active = 1"""))
                df = pd.DataFrame(result.fetchall())
                df.columns = result.keys()

                for index, row in df.iterrows():
                    stg_customer = StgCustomer(bk=row['customer_id'],
                                               store_bk=row['store_id'],
                                               first_name=row['first_name'],
                                               last_name=row['last_name'],
                                               email=row['email'],
                                               address_bk=row['address_id'],
                                               active=True)
                    stg_customer.format_stg_costumer()
                    session.add(stg_customer)
                    session.commit()
                df_final = pd.DataFrame(session.query(StgCustomer).all())
            except SQLAlchemyError as sql_ex:
                logging.error(sql_ex)
                session.rollback()
            except Exception as ex:
                logging.error(ex)
                session.rollback()
        return df_final

    @classmethod
    def load_dim_from_db(cls) -> list[DimCustomer]:
        engine, session_context = get_connection()

        dim: list[DimCustomer] = None
        with session_context() as session:
            try:
                dim = session.query(DimCustomer).all()
            except SQLAlchemyError as sql_ex:
                logging.error(sql_ex)
                session.rollback()
            except Exception as ex:
                logging.error(ex)
                session.rollback()
        return dim

    @classmethod
    def find_item_by_bk(cls, bk) -> DimCustomer:
        engine, session_context = get_connection()

        dim_item: DimCustomer = None
        with session_context() as session:
            try:
                dim_item = session.query(DimCustomer).filter_by(bk=bk).first()
            except SQLAlchemyError as sql_ex:
                logging.error(sql_ex)
                session.rollback()
            except Exception as ex:
                logging.error(ex)
                session.rollback()
        return dim_item

    @classmethod
    def get_sk_information(cls, store_bk, address_bk):
        engine, session_context = get_connection()

        store = Store()
        address = Address()

        store_sk = cls.INVALID_DATA
        address_sk = cls.INVALID_DATA

        with session_context() as session:
            try:
                dim_address = address.find_item_by_bk(address_bk)
                address_sk = dim_address.sk if dim_address is not None else cls.INVALID_DATA

                dim_store = store.find_item_by_bk(store_bk)
                store_sk = dim_store.sk if dim_store is not None else cls.INVALID_DATA
            except SQLAlchemyError as sql_ex:
                logging.error(sql_ex)
                session.rollback()
            except Exception as ex:
                logging.error(ex)
                session.rollback()
        return store_sk, address_sk
    
    @classmethod
    def load_dim(cls) -> pd.DataFrame:
        list_dim_customer = cls.load_dim_from_db()

        engine, session_context = get_connection()
        df = pd.DataFrame()
        with session_context() as session:
            try:
                s_customer = session.query(StgCustomer).all()
                df = pd.DataFrame([vars(customer) for customer in s_customer])

                result_update = [stg for stg in s_customer if
                                 stg.bk in [dim.bk for dim in list_dim_customer]]
                result_insert = [stg for stg in s_customer if
                                 stg.bk not in [dim.bk for dim in list_dim_customer]]
                list_dim_customer.clear()

                for stg in result_update:
                    store_sk, address_sk = cls.get_sk_information(stg.store_bk, stg.address_bk)

                    upd_customer = session.query(DimCustomer).filter_by(bk=stg.bk).first()
                    upd_customer.store_sk = store_sk
                    upd_customer.address_sk = address_sk
                    upd_customer.set_full_name(stg.first_name, stg.last_name)
                    upd_customer.email = stg.email
                    upd_customer.active = stg.active
                    session.commit()
                    del upd_customer

                for stg in result_insert:
                    store_sk, address_sk = cls.get_sk_information(stg.store_bk, stg.address_bk)

                    dim_customer = DimCustomer(bk=stg.bk,
                                               store_sk=store_sk,
                                               address_sk=store_sk,
                                               email=stg.email,
                                               active=stg.active)
                    dim_customer.set_full_name(stg.first_name, stg.last_name)
                    session.add(dim_customer)
                    session.commit()
            except SQLAlchemyError as sql_ex:
                logging.error(sql_ex)
                session.rollback()
            except Exception as ex:
                logging.error(ex)
                session.rollback()
        return df
    