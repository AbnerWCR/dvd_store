from modules.country import Country
from infra.db_connection import get_connection
from models.stg.stg_country import StgCountry
from models.dim.dim_country import DimCountry


def test_country_load_stg():
    country = Country()
    df = country.load_stg()
    assert not df.empty


def test_country_load_dim_from_db():
    country = Country()
    dim = country.load_dim_from_db()
    assert len(dim) >= 0


def test_country_load_dim_with_new_data():
    country = Country()
    df = country.load_dim()
    assert not df.empty


def test_change_dim_country():
    engine, session_context = get_connection()

    with session_context() as session:
        result = session.query(StgCountry).filter_by(bk=-10).first()

        if result is not None:
            session.delete(result)
            session.commit()

        stg = StgCountry(bk=-10, country="invalidd")
        session.add(stg)
        session.commit()

        country = Country()
        df = country.load_dim()
        result = df[df['bk'] == -10]
        assert not result.empty
        assert (result['country'] == "invalidd").all()

        new_dim = session.query(DimCountry).filter_by(bk=-10).first()
        assert new_dim.country == "Invalidd"

        new_stg = session.query(StgCountry).filter_by(bk=-10).first()
        new_stg.country = "invalid"
        session.commit()

        del df
        df = country.load_dim()
        result = df[df['bk'] == -10]
        assert not result.empty
        assert (result['country'] == "invalid").all()
        new_dim = session.query(DimCountry).filter_by(bk=-10).first()
        assert new_dim.country == "Invalid"

