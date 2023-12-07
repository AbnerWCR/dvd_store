import logging

import pandas as pd
from sqlalchemy import text, insert
from infra.db_connection import get_connection
from models.stg.stg_actor import StgActor
from models.dim.dim_actor import DimActor
from modules.base_module import IBaseModule


class Actor(IBaseModule):

    @classmethod
    def find_item_by_bk(cls, bk) -> DimActor:
        engine, session_context = get_connection()

        dim_item: DimActor = None
        with session_context() as session:
            try:
                dim_item = session.query(DimActor).filter_by(bk=bk).first()
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
                result = session.execute(text(f"""select * from public.actor"""))
                df = pd.DataFrame(result.fetchall())
                df.columns = result.keys()

                for index, row in df.iterrows():
                    stg_actor = StgActor(bk=row['actor_id'],
                                         first_name=row['first_name'],
                                         last_name=row['last_name'])

                    stg_actor.format_stg_actor()
                    session.add(stg_actor)
                    session.commit()
            except Exception as ex:
                logging.error(ex)
                session.rollback()

        return df

    @classmethod
    def load_dim_from_db(cls) -> list[DimActor]:
        engine, session_context = get_connection()

        dim: list[DimActor] = []
        with session_context() as session:
            try:
                dim = session.query(DimActor).all()
            except Exception as ex:
                logging.error(ex)
                session.rollback()

        return dim

    @classmethod
    def load_dim(cls) -> pd.DataFrame:
        list_dim_actor = cls.load_dim_from_db()

        engine, session_context = get_connection()
        df = pd.DataFrame()
        with session_context() as session:
            try:
                s_actor = session.query(StgActor).all()
                df = pd.DataFrame([vars(actor) for actor in s_actor])

                result_update = [stg for stg in s_actor if
                                 stg.bk in [dim.bk for dim in list_dim_actor]]
                result_insert = [stg for stg in s_actor if
                                 stg.bk not in [dim.bk for dim in list_dim_actor]]
                list_dim_actor.clear()

                for stg in result_update:
                    dim_actor = DimActor()
                    dim_actor.create_dim_from_stg(stg)

                    upd_actor = session.query(DimActor).filter_by(bk=stg.bk).first()
                    upd_actor.full_name = dim_actor.full_name
                    session.commit()
                    del upd_actor

                for stg in result_insert:
                    dim_actor = DimActor()
                    dim_actor.create_dim_from_stg(stg)
                    session.add(dim_actor)
                    session.commit()
            except Exception as ex:
                logging.error(ex)
                session.rollback()

        return df
