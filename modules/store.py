from modules.base_module import IBaseModule
from infra.db_connection import get_connection
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from models.stg.stg_store import StgStore
from models.dim.dim_store import DimStore
from modules.address import Address
import pandas as pd
import logging


class Store(IBaseModule):

    @classmethod
    def load_stg(cls) -> pd.DataFrame:
        engine, session_context = get_connection()

        df_final = pd.DataFrame()
        with session_context() as session:
            try:
                session.query(StgStore).delete()
                session.commit()

                result = session.execute(text(f"""select str.*, (sta.first_name || ' ' || sta.last_name) manager from public.store str
                                                  inner join public.staff sta on str.manager_staff_id = sta.staff_id"""))
                df = pd.DataFrame(result.fetchall())
                df.columns = result.keys()

                for index, row in df.iterrows():
                    stg_store = StgStore(bk=row['store_id'],
                                         manager=row['manager'],
                                         address_bk=row['address_id'])

                    stg_store.format_stg_store()
                    session.add(stg_store)
                    session.commit()
                df_final = pd.DataFrame(session.query(StgStore).all())
            except Exception as ex:
                logging.error(ex)
                session.rollback()
        return df_final

    @classmethod
    def load_dim_from_db(cls) -> list[DimStore]:
        engine, session_context = get_connection()

        dim: list[DimStore] = []
        with session_context() as session:
            try:
                dim = session.query(DimStore).all()
            except Exception as ex:
                logging.error(ex)
                session.rollback()
        return dim

    @classmethod
    def find_item_by_bk(cls, bk) -> DimStore:
        engine, session_context = get_connection()

        dim_item: DimStore = None
        with session_context() as session:
            try:
                dim_item = session.query(DimStore).filter_by(bk=bk).first()
            except Exception as ex:
                logging.error(ex)
                session.rollback()
        return dim_item

    @classmethod
    def get_sk_information(cls, address_bk):
        engine, session_context = get_connection()

        address = Address()

        address_sk = cls.INVALID_DATA

        with session_context() as session:
            try:
                dim_address = address.find_item_by_bk(address_bk)
                address_sk = dim_address.sk if dim_address is not None else cls.INVALID_DATA
            except SQLAlchemyError as sql_ex:
                logging.error(sql_ex)
                session.rollback()
            except Exception as ex:
                logging.error(ex)
                session.rollback()
        return address_sk

    @classmethod
    def load_dim(cls) -> pd.DataFrame:
        list_dim_country = cls.load_dim_from_db()

        engine, session_context = get_connection()
        df = pd.DataFrame()
        with session_context() as session:
            try:
                s_store = session.query(StgStore).all()
                df = pd.DataFrame([vars(store) for store in s_store])

                result_update = [stg for stg in s_store if
                                 stg.bk in [dim.bk for dim in list_dim_country]]
                result_insert = [stg for stg in s_store if
                                 stg.bk not in [dim.bk for dim in list_dim_country]]
                list_dim_country.clear()

                for stg in result_update:
                    address_sk = cls.get_sk_information(stg.address_bk)
                    upd_store = session.query(DimStore).filter_by(bk=stg.bk).first()
                    upd_store.address_bk = address_sk
                    upd_store.manager = stg.manager
                    upd_store.format_store()
                    session.commit()
                    del upd_store

                for stg in result_insert:
                    address_sk = cls.get_sk_information(stg.address_bk)
                    dim_store = DimStore(bk=stg.bk,
                                         manager=stg.manager,
                                         address_sk=address_sk)
                    dim_store.format_store()
                    session.add(dim_store)
                    session.commit()
            except Exception as ex:
                logging.error(ex)
                session.rollback()
        return df
