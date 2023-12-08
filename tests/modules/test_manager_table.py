from sqlalchemy import ExceptionContext
from infra.db_connection import get_connection
import logging


def test_create_tables():
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
    import models.stg.stg_customer
    import models.dim.dim_customer
    import models.stg.stg_country
    import models.dim.dim_country
    import models.stg.stg_film
    import models.dim.dim_film
    import models.stg.stg_language
    import models.dim.dim_language
    import models.stg.stg_store
    import models.dim.dim_store

    Base.metadata.create_all(engine)
    assert True
