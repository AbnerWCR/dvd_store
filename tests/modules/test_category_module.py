from modules.category import Category
from infra.db_connection import get_connection
from models.stg.stg_category import StgCategory
from models.dim.dim_category import DimCategory


def test_category_load_stg():
    category = Category()
    df = category.load_stg()
    assert not df.empty


def test_category_load_dim_from_db():
    category = Category()
    dim = category.load_dim_from_db()
    assert len(dim) >= 0


def test_category_load_dim_with_new_data():
    category = Category()
    df = category.load_dim()
    assert not df.empty


def test_change_dim_category():
    engine, session_context = get_connection()

    with session_context() as session:
        result = session.query(StgCategory).filter_by(bk=-10).first()

        if result is not None:
            session.delete(result)
            session.commit()

        stg = StgCategory(bk=-10, name="invalidd")
        session.add(stg)
        session.commit()

        category = Category()
        df = category.load_dim()
        result = df[df['bk'] == -10]
        assert not result.empty
        assert (result['name'] == "invalidd").all()

        new_dim = session.query(DimCategory).filter_by(bk=-10).first()
        assert new_dim.name == "Invalidd"

        new_stg = session.query(StgCategory).filter_by(bk=-10).first()
        new_stg.name = "invalid"
        session.commit()

        del df
        df = category.load_dim()
        result = df[df['bk'] == -10]
        assert not result.empty
        assert (result['name'] == "invalid").all()
        new_dim = session.query(DimCategory).filter_by(bk=-10).first()
        assert new_dim.name == "Invalid"

        session.delete(new_dim)
        session.commit()
