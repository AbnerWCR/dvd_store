import logging
from modules.base_module import IBaseModule
import pandas as pd
from sqlalchemy import text, insert
from infra.db_connection import get_connection
from models.stg.stg_address import StgAddress
from models.dim.dim_address import DimAddress
from modules.city import City


class Address(IBaseModule):

    @classmethod
    def find_item_by_bk(cls, bk) -> DimAddress:
        engine, session_context = get_connection()

        dim_item: DimAddress = None
        with session_context() as session:
            try:
                dim_item = session.query(DimAddress).filter_by(bk=bk).first()
            except Exception as ex:
                logging.error(ex)
                session.rollback()
        return dim_item

    @classmethod
    def load_stg(cls) -> pd.DataFrame:
        engine, session_context = get_connection()

        df = pd.DataFrame()
        with session_context() as session:
            try:
                session.query(StgAddress).delete()
                session.commit()

                result = session.execute(text("select * from public.address"))
                df = pd.DataFrame(result.fetchall())
                df.columns = result.keys()

                for index, row in df.iterrows():
                    stg_address = StgAddress(bk=row['address_id'],
                                             address=row['address'],
                                             address2=row['address2'],
                                             district=row['district'],
                                             city_bk=row['city_bk'])
                    stg_address.format_stg_address()
                    session.add(stg_address)
                    session.commit()
            except Exception as ex:
                logging.error(ex)
                session.rollback()
        return df

    @classmethod
    def load_dim_from_db(cls) -> list[DimAddress]:
        engine, session_context = get_connection()

        dim: list[DimAddress] = []
        with session_context() as session:
            try:
                dim = session.query(DimAddress).all()
            except Exception as ex:
                logging.error(ex)
                session.rollback()
        return dim

    @classmethod
    def load_dim(cls) -> pd.DataFrame:
        list_dim_address = cls.load_dim_from_db()

        engine, session_context = get_connection()

        df = pd.DataFrame()
        with session_context() as session:
            try:
                s_address = session.query(StgAddress).all()
                df = pd.DataFrame([vars(address) for address in s_address])

                result_update = [stg for stg in s_address if
                                 stg.bk in [dim.bk for dim in list_dim_address]]
                result_insert = [stg for stg in s_address if
                                 stg.bk not in [dim.bk for dim in list_dim_address]]
                list_dim_address.clear()

                city = City()
                for stg in result_update:
                    dim_city = city.find_city_from_bk(stg.city_bk)

                    upd_address = session.query(DimAddress).filter_by(bk=stg.bk).first()
                    upd_address.city_sk = dim_city.bk if dim_city is not None else cls.INVALID_DATA
                    upd_address.address = stg.address
                    upd_address.address2 = stg.address2
                    upd_address.district = stg.district
                    upd_address.format_address()
                    session.commit()
                    del dim_city
                    del upd_address
                for stg in result_insert:
                    dim_city = city.find_city_from_bk(stg.city_bk)
                    city_sk = dim_city.sk if dim_city is not None else cls.INVALID_DATA

                    dim_address = DimAddress(city_sk=city_sk,
                                             address=stg.address,
                                             address2=stg.address2,
                                             district=stg.district)
                    dim_address.format_address()
                    session.add(dim_address)
                    session.commit()
            except Exception as ex:
                logging.error(ex)
                session.rollback()
        return df
    