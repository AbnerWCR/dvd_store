from modules.base_module import IBaseModule
from infra.db_connection import get_connection
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from models.stg.stg_language import StgLanguage
from models.dim.dim_language import DimLanguage
import pandas as pd
import logging


class Language(IBaseModule):

    @classmethod
    def load_stg(cls) -> pd.DataFrame:
        engine, session_context = get_connection()

        df = pd.DataFrame()
        with session_context() as session:
            try:
                session.query(StgLanguage).delete()
                session.commit()

                result = session.execute(text(f"""select * from public.language"""))
                df = pd.DataFrame(result.fetchall())
                df.columns = result.keys()

                for index, row in df.iterrows():
                    stg_language = StgLanguage(bk=row['language_id'],
                                               name=row['name'])

                    stg_language.format_stg_language()
                    session.add(stg_language)
                    session.commit()
            except Exception as ex:
                logging.error(ex)
                session.rollback()
        return df

    @classmethod
    def load_dim_from_db(cls) -> list[DimLanguage]:
        engine, session_context = get_connection()

        dim: list[DimLanguage] = []
        with session_context() as session:
            try:
                dim = session.query(DimLanguage).all()
            except Exception as ex:
                logging.error(ex)
                session.rollback()
        return dim

    @classmethod
    def find_item_by_bk(cls, bk) -> DimLanguage:
        engine, session_context = get_connection()

        dim_item: DimLanguage = None
        with session_context() as session:
            try:
                dim_item = session.query(DimLanguage).filter_by(bk=bk).first()
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
                s_language = session.query(StgLanguage).all()
                df = pd.DataFrame([vars(lang) for lang in s_language])

                result_update = [stg for stg in s_language if
                                 stg.bk in [dim.bk for dim in list_dim_country]]
                result_insert = [stg for stg in s_language if
                                 stg.bk not in [dim.bk for dim in list_dim_country]]
                list_dim_country.clear()

                for stg in result_update:
                    upd_lang = session.query(DimLanguage).filter_by(bk=stg.bk).first()
                    upd_lang.name = stg.name
                    upd_lang.format_language()
                    session.commit()
                    del upd_lang

                for stg in result_insert:
                    dim_lang = DimLanguage(bk=stg.bk,
                                           name=stg.name)
                    dim_lang.format_language()
                    session.add(dim_lang)
                    session.commit()
            except Exception as ex:
                logging.error(ex)
                session.rollback()
        return df
