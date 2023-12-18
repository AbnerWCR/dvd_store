import logging
from modules.base_module import IBaseModule
import pandas as pd
from sqlalchemy import text, insert
from infra.db_connection import get_connection
from models.stg.stg_city import StgCity
from models.dim.dim_city import DimCity
from modules.country import Country


class City(IBaseModule):

    @classmethod
    def find_item_by_bk(cls, bk) -> DimCity:
        engine, session_context = get_connection()

        dim_item: DimCity = None
        with session_context() as session:
            try:
                dim_item = session.query(DimCity).filter_by(bk=bk).first()
            except Exception as ex:
                logging.error(ex)
                session.rollback()
        return dim_item

    @classmethod
    def load_stg(cls) -> pd.DataFrame | None:
        engine, session_context = get_connection()

        df = pd.DataFrame()
        with session_context() as session:
            try:
                session.query(StgCity).delete()
                session.commit()

                result = session.execute(text(f"""select * from public.city"""))
                df = pd.DataFrame(result.fetchall())
                df.columns = result.keys()

                for index, row in df.iterrows():
                    stg_city = StgCity(bk=row['city_id'],
                                       city=row['city'],
                                       country_bk=row['country_id'])

                    stg_city.format_stg_city()
                    session.add(stg_city)
                    session.commit()
            except Exception as ex:
                logging.error(ex)
                session.rollback()
                return None
        return df

    @classmethod
    def load_dim_from_db(cls) -> list[DimCity]:
        engine, session_context = get_connection()

        dim: list[DimCity] = []
        with session_context() as session:
            try:
                dim = session.query(DimCity).all()
            except Exception as ex:
                logging.error(ex)
                session.rollback()
        return dim

    @classmethod
    def load_dim(cls) -> pd.DataFrame | None:
        list_dim_city = cls.load_dim_from_db()

        engine, session_context = get_connection()
        df = pd.DataFrame()
        with session_context() as session:
            try:
                s_city = session.query(StgCity).all()
                df = pd.DataFrame([vars(city) for city in s_city])

                result_update = [stg for stg in s_city if
                                 stg.bk in [dim.bk for dim in list_dim_city]]
                result_insert = [stg for stg in s_city if
                                 stg.bk not in [dim.bk for dim in list_dim_city]]
                list_dim_city.clear()

                country = Country()
                for stg in result_update:
                    dim_country = country.find_item_by_bk(stg.country_bk)

                    upd_city = session.query(DimCity).filter_by(bk=stg.bk).first()
                    upd_city.city = stg.city
                    upd_city.country_sk = dim_country.sk if dim_country is not None else cls.INVALID_DATA
                    upd_city.format_city()
                    session.commit()
                    del upd_city

                for stg in result_insert:
                    dim_country = country.find_item_by_bk(stg.country_bk)
                    country_sk = dim_country.sk if dim_country is not None else cls.INVALID_DATA

                    dim_city = DimCity(bk=stg.bk,
                                       city=stg.city,
                                       country_sk=country_sk)
                    dim_city.format_city()
                    session.add(dim_city)
                    session.commit()
            except Exception as ex:
                logging.error(ex)
                session.rollback()
                return None
        return df
