import logging

import pandas as pd
from sqlalchemy import text, insert
from infra.db_connection import get_connection
from models.stg.stg_category import StgCategory
from models.dim.dim_category import DimCategory


class Category:

    @classmethod
    def load_stg(cls) -> pd.DataFrame:
        engine, session_context = get_connection()

        df = pd.DataFrame()
        with session_context() as session:
            try:
                result = session.execute(text(f"""select * from public.category"""))
                df = pd.DataFrame(result.fetchall())
                df.columns = result.keys()

                for index, row in df.iterrows():
                    stg_category = StgCategory(bk=row['category_id'],
                                               name=row['name'], )

                    stg_category.format_stg_category()
                    session.add(stg_category)
                    session.commit()
            except Exception as ex:
                logging.error(ex)
                session.rollback()
        return df

    @classmethod
    def load_dim_from_db(cls) -> list[DimCategory]:
        engine, session_context = get_connection()

        dim: list[DimCategory] = []
        with session_context() as session:
            try:
                dim = session.query(DimCategory).all()
            except Exception as ex:
                logging.error(ex)
                session.rollback()
        return dim

    @classmethod
    def find_category_from_bk(cls, bk: int) -> DimCategory:
        engine, session_context = get_connection()

        dim_item: DimCategory = None
        with session_context() as session:
            try:
                dim_item = session.query(DimCategory).filter_by(bk=bk).first()
            except Exception as ex:
                logging.error(ex)
                session.rollback()
        return dim_item

    @classmethod
    def load_dim(cls) -> pd.DataFrame:
        list_dim_category = cls.load_dim_from_db()

        engine, session_context = get_connection()
        df = pd.DataFrame()
        with session_context() as session:
            try:
                s_category = session.query(StgCategory).all()
                df = pd.DataFrame([vars(category) for category in s_category])

                result_update = [stg for stg in s_category if
                                 stg.bk in [dim.bk for dim in list_dim_category]]
                result_insert = [stg for stg in s_category if
                                 stg.bk not in [dim.bk for dim in list_dim_category]]
                list_dim_category.clear()

                for stg in result_update:
                    dim_category = DimCategory()
                    dim_category.create_dim_from_stg(stg)

                    upd_category = session.query(DimCategory).filter_by(bk=stg.bk).first()
                    upd_category.name = dim_category.name
                    session.commit()
                    del upd_category

                for stg in result_insert:
                    dim_category = DimCategory()
                    dim_category.create_dim_from_stg(stg)
                    session.add(dim_category)
                    session.commit()
            except Exception as ex:
                logging.error(ex)
                session.rollback()
        return df
