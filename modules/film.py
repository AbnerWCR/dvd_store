from modules.base_module import IBaseModule
from infra.db_connection import get_connection
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from models.stg.stg_film import StgFilm
from models.dim.dim_film import DimFilm
from modules.category import Category
from modules.language import Language
from modules.actor import Actor
import pandas as pd
import logging


class Film(IBaseModule):

    @classmethod
    def load_stg(cls) -> pd.DataFrame:
        engine, session_context = get_connection()
        df = pd.DataFrame()

        with session_context() as session:
            try:
                session.query(StgFilm).delete()
                session.commit()

                result = session.execute(text(f"""select f.*, -1 as actor_id, fc.category_id from public.film f
                                                  --inner join public.film_actor fa on f.film_id = fa.film_id
                                                  inner join public.film_category fc on f.film_id = fc.film_id"""))
                df = pd.DataFrame(result.fetchall())
                df.columns = result.keys()

                for index, row in df.iterrows():
                    stg_film = StgFilm(bk=row['film_id'],
                                       title=row['title'],
                                       release_year=row['release_year'],
                                       rental_duration=row['rental_duration'],
                                       rental_rate=row['rental_rate'],
                                       length=row['length'],
                                       replacement_cost=row['replacement_cost'],
                                       rating=row['rating'],
                                       language_bk=row['language_id'],
                                       original_language_bk=row['original_language_id'],
                                       actor_bk=row['actor_id'],
                                       category_bk=row['category_id'])
                    stg_film.format_stg_film()
                    session.add(stg_film)
                    session.commit()
            except SQLAlchemyError as sql_ex:
                logging.error(sql_ex)
                session.rollback()
            except Exception as ex:
                logging.error(ex)
                session.rollback()
        return df

    @classmethod
    def find_item_by_bk(cls, bk) -> DimFilm:
        engine, session_context = get_connection()

        dim_item: DimFilm = None
        with session_context() as session:
            try:
                dim_item = session.query(DimFilm).filter_by(bk=bk).first()
            except SQLAlchemyError as sql_ex:
                logging.error(sql_ex)
                session.rollback()
            except Exception as ex:
                logging.error(ex)
                session.rollback()
        return dim_item

    @classmethod
    def load_dim_from_db(cls) -> list[DimFilm]:
        engine, session_context = get_connection()

        dim: list[DimFilm] = []
        with session_context() as session:
            try:
                dim = session.query(DimFilm).all()
            except SQLAlchemyError as sql_ex:
                logging.error(sql_ex)
                session.rollback()
            except Exception as ex:
                logging.error(ex)
                session.rollback()
        return dim

    @classmethod
    def get_sk_information(cls, actor_bk, category_bk, language_bk, orig_lang_bk):
        engine, session_context = get_connection()

        category = Category()
        actor = Actor()
        language = Language()

        category_sk = cls.INVALID_DATA
        actor_sk = cls.INVALID_DATA
        language_sk = cls.INVALID_DATA
        o_language_sk = cls.INVALID_DATA

        with session_context() as session:
            try:
                dim_category = category.find_category_from_bk(category_bk)
                category_sk = dim_category.sk if dim_category is not None else cls.INVALID_DATA

                dim_actor = actor.find_item_by_bk(actor_bk)
                actor_sk = dim_actor.sk if dim_actor is not None else cls.INVALID_DATA

                dim_language = language.find_item_by_bk(language_bk)
                language_sk = dim_language.sk if dim_language is not None else cls.INVALID_DATA

                dim_o_language = language.find_item_by_bk(orig_lang_bk)
                o_language_sk = dim_o_language.sk if dim_o_language is not None else cls.INVALID_DATA
            except SQLAlchemyError as sql_ex:
                logging.error(sql_ex)
                session.rollback()
            except Exception as ex:
                logging.error(ex)
                session.rollback()
        return actor_sk, category_sk, language_sk, o_language_sk

    @classmethod
    def load_dim(cls) -> pd.DataFrame:
        list_dim_film = cls.load_dim_from_db()

        engine, session_context = get_connection()
        df = pd.DataFrame()
        with session_context() as session:
            try:
                s_film = session.query(StgFilm).all()
                df = pd.DataFrame([vars(film) for film in s_film])

                result_update = [stg for stg in s_film if
                                 stg.bk in [dim.bk for dim in list_dim_film]]
                result_insert = [stg for stg in s_film if
                                 stg.bk not in [dim.bk for dim in list_dim_film]]
                list_dim_film.clear()

                for stg in result_update:
                    actor_sk, category_sk, language_sk, o_language_sk = cls.get_sk_information(
                        stg.actor_bk, stg.category_bk, stg.language_bk, stg.original_language_bk
                    )

                    upd_film = session.query(DimFilm).filter_by(bk=stg.bk).first()
                    upd_film.title = stg.title
                    upd_film.release_year = stg.release_year
                    upd_film.rental_duration = stg.rental_duration
                    upd_film.rental_rate = stg.rental_rate
                    upd_film.length = stg.length
                    upd_film.replacement_cost = stg.replacement_cost
                    upd_film.rating = stg.rating
                    upd_film.language_sk = language_sk
                    upd_film.original_language_sk = o_language_sk
                    upd_film.actor_sk = actor_sk
                    upd_film.category_sk = category_sk
                    upd_film.format_film()
                    session.commit()
                    del upd_film

                for stg in result_insert:
                    actor_sk, category_sk, language_sk, o_language_sk = cls.get_sk_information(
                        stg.actor_bk, stg.category_bk, stg.language_bk, stg.original_language_bk
                    )

                    dim_category = DimFilm(bk=stg.bk,
                                           title=stg.title,
                                           release_year=stg.release_year,
                                           language_sk=language_sk,
                                           original_language_sk=o_language_sk,
                                           rental_duration=stg.rental_duration,
                                           rental_rate=stg.rental_rate,
                                           length=stg.length,
                                           replacement_cost=stg.replacement_cost,
                                           actor_sk=actor_sk,
                                           category_sk=category_sk,
                                           rating=stg.rating)
                    dim_category.format_film()
                    session.add(dim_category)
                    session.commit()
            except SQLAlchemyError as sql_ex:
                logging.error(sql_ex)
                session.rollback()
            except Exception as ex:
                logging.error(ex)
                session.rollback()
        return df
