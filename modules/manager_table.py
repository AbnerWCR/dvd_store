from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import ExceptionContext
from infra.db_connection import get_connection
import logging

Base = declarative_base()


def create_tables():
    try:
        engine, session_context = get_connection()
        from models.base_model import Base
        import models.stg.stg_actor
        import models.dim.dim_actor
        import models.stg.stg_address
        import models.dim.dim_address
        import models.stg.stg_category
        import models.dim.dim_category
        import models.stg.stg_city
        import models.dim.dim_city
        import models.stg.stg_costumer
        import models.dim.dim_costumer
        import models.stg.stg_country
        import models.dim.dim_country
        import models.stg.stg_film
        import models.dim.dim_film
        import models.stg.stg_language
        import models.dim.dim_language
        import models.stg.stg_store
        import models.dim.dim_store
        import models.fact.fact_payment

        Base.metadata.create_all(engine)
    except ExceptionContext as ex:
        logging.error(ex)
    except Exception as e:
        logging.error(e)


def set_default_invalid_data():
    import models.dim.dim_actor as actor
    import models.dim.dim_address as address
    import models.dim.dim_category as category
    import models.dim.dim_city as city
    import models.dim.dim_customer as customer
    import models.dim.dim_country as country
    import models.dim.dim_film as film
    import models.dim.dim_language as language
    import models.dim.dim_store as store

    engine, session_context = get_connection()
    with session_context() as session:
        try:
            pass
        except ExceptionContext as ex:
            logging.error(ex)
        except Exception as e:
            logging.error(e)