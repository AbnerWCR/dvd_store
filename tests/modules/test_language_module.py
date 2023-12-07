from modules.language import Language
from infra.db_connection import get_connection
from models.stg.stg_language import StgLanguage
from models.dim.dim_language import DimLanguage


def test_language_load_stg():
    lang = Language()
    df = lang.load_stg()
    assert not df.empty


def test_language_load_dim_from_db():
    lang = Language()
    dim = lang.load_dim_from_db()
    assert len(dim) >= 0


def test_language_load_dim_with_new_data():
    lang = Language()
    df = lang.load_dim()
    assert not df.empty


def test_change_dim_language():
    engine, session_context = get_connection()

    with session_context() as session:
        result = session.query(StgLanguage).filter_by(bk=-10).first()

        if result is not None:
            session.delete(result)
            session.commit()

        stg = StgLanguage(bk=-10, name="invalidd")
        session.add(stg)
        session.commit()

        lang = Language()
        df = lang.load_dim()
        result = df[df['bk'] == -10]
        assert not result.empty
        assert (result['name'] == "invalidd").all()

        new_dim = session.query(DimLanguage).filter_by(bk=-10).first()
        assert new_dim.name == "Invalidd"

        new_stg = session.query(StgLanguage).filter_by(bk=-10).first()
        new_stg.name = "invalid"
        session.commit()

        del df
        df = lang.load_dim()
        result = df[df['bk'] == -10]
        assert not result.empty
        assert (result['name'] == "invalid").all()
        new_dim = session.query(DimLanguage).filter_by(bk=-10).first()
        assert new_dim.name == "Invalid"

        session.delete(new_dim)
        session.commit()
