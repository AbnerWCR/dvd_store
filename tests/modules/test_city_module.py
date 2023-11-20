from modules.city import City
from infra.db_connection import get_connection
from models.stg.stg_city import StgCity
from models.dim.dim_city import DimCity


def test_city_load_stg():
    city = City()
    df = city.load_stg()
    assert not df.empty


def test_city_load_dim_from_db():
    city = City()
    dim = city.load_dim_from_db()
    assert len(dim) >= 0


def test_city_load_dim_with_new_data():
    city = City()
    df = city.load_dim()
    assert not df.empty


def test_change_dim_city():
    engine, session_context = get_connection()

    with session_context() as session:
        result = session.query(StgCity).filter_by(bk=-10).first()

        if result is not None:
            session.delete(result)
            session.commit()

        stg = StgCity(bk=-10, city="invalidd", country_bk=-10)
        session.add(stg)
        session.commit()

        city = City()
        df = city.load_dim()
        result = df[df['bk'] == -10]
        assert not result.empty
        assert (result['city'] == "invalidd").all()

        new_dim = session.query(DimCity).filter_by(bk=-10).first()
        assert new_dim.city == "Invalidd"

        new_stg = session.query(StgCity).filter_by(bk=-10).first()
        new_stg.city = "invalid"
        session.commit()

        del df
        df = city.load_dim()
        result = df[df['bk'] == -10]
        assert not result.empty
        assert (result['city'] == "invalid").all()
        new_dim = session.query(DimCity).filter_by(bk=-10).first()
        assert new_dim.city == "Invalid"

