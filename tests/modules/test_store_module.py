from modules.store import Store
from infra.db_connection import get_connection
from models.stg.stg_store import StgStore
from models.dim.dim_store import DimStore


def test_store_load_stg():
    store = Store()
    df = store.load_stg()
    assert not df.empty


def test_store_load_dim_from_db():
    store = Store()
    dim = store.load_dim_from_db()
    assert len(dim) >= 0


def test_store_load_dim_with_new_data():
    store = Store()
    df = store.load_dim()
    assert not df.empty


def test_change_dim_store():
    engine, session_context = get_connection()

    with session_context() as session:
        result = session.query(StgStore).filter_by(bk=-10).first()

        if result is not None:
            session.delete(result)
            session.commit()

        stg = StgStore(bk=-10, manager="invalidd valUe", address_bk=-10)
        stg.format_stg_store()
        session.add(stg)
        session.commit()

        store = Store()
        df = store.load_dim()
        result = df[df['bk'] == -10]
        assert not result.empty
        assert (result['manager'] == "invalidd value").all()
        assert (result['address_bk'] == -10).all()

        new_dim = session.query(DimStore).filter_by(bk=-10).first()
        assert new_dim.manager == "Invalidd Value"
        assert new_dim.address_sk == -1

        new_stg = session.query(StgStore).filter_by(bk=-10).first()
        new_stg.manager = "invalid valUE"
        new_stg.format_stg_store()
        session.commit()

        del df
        df = store.load_dim()
        result = df[df['bk'] == -10]
        assert not result.empty
        assert (result['manager'] == "invalid value").all()
        assert (result['address_bk'] == -10).all()
        new_dim = session.query(DimStore).filter_by(bk=-10).first()
        assert new_dim.manager == "Invalid Value"
        assert new_dim.address_sk == -1

        session.delete(new_dim)
        session.commit()
