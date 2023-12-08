import logging

import pandas as pd
from sqlalchemy import text, insert
from infra.db_connection import get_connection
from models.stg.stg_country import StgCountry
from models.dim.dim_country import DimCountry


class Country:

    @classmethod
    def load_stg(cls) -> pd.DataFrame:
        engine, session_context = get_connection()

        df = pd.DataFrame()
        with session_context() as session:
            try:
                session.query(StgCountry).delete()
                session.commit()

                result = session.execute(text(f"""select * from public.country"""))
                df = pd.DataFrame(result.fetchall())
                df.columns = result.keys()

                for index, row in df.iterrows():
                    stg_country = StgCountry(bk=row['country_id'],
                                             country=row['country'], )

                    stg_country.format_stg_country()
                    session.add(stg_country)
                    session.commit()
            except Exception as ex:
                logging.error(ex)
                session.rollback()
        return df

    @classmethod
    def load_dim_from_db(cls) -> list[DimCountry]:
        engine, session_context = get_connection()

        dim: list[DimCountry] = []
        with session_context() as session:
            try:
                dim = session.query(DimCountry).all()
            except Exception as ex:
                logging.error(ex)
                session.rollback()
        return dim

    @classmethod
    def find_coutry_from_bk(cls, bk: int) -> DimCountry:
        engine, session_context = get_connection()

        dim_item: DimCountry = None
        with session_context() as session:
            try:
                dim_item = session.query(DimCountry).filter_by(bk=bk).first()
            except Exception as ex:
                logging.error(ex)
                session.rollback()
        return dim_item

    @classmethod
    def load_dim(cls) -> pd.DataFrame:
        list_dim_country = cls.load_dim_from_db()

        engine, session_context = get_connection()
        df = pd.DataFrame()
        with session_context() as session:
            try:
                s_country = session.query(StgCountry).all()
                df = pd.DataFrame([vars(country) for country in s_country])

                result_update = [stg for stg in s_country if
                                 stg.bk in [dim.bk for dim in list_dim_country]]
                result_insert = [stg for stg in s_country if
                                 stg.bk not in [dim.bk for dim in list_dim_country]]
                list_dim_country.clear()

                for stg in result_update:
                    dim_country = DimCountry()
                    dim_country.create_dim_from_stg(stg)

                    upd_country = session.query(DimCountry).filter_by(bk=stg.bk).first()
                    upd_country.country = dim_country.country
                    session.commit()
                    del upd_country

                for stg in result_insert:
                    dim_country = DimCountry()
                    dim_country.create_dim_from_stg(stg)
                    session.add(dim_country)
                    session.commit()
            except Exception as ex:
                logging.error(ex)
                session.rollback()
        return df
