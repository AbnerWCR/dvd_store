from modules.actor import Actor
from infra.db_connection import get_connection
from models.stg.stg_actor import StgActor
from models.dim.dim_actor import DimActor


def test_actor_load_stg():
    actor = Actor()
    df = actor.load_stg()
    assert not df.empty


def test_actor_load_dim_from_db():
    actor = Actor()
    dim = actor.load_dim_from_db()
    assert len(dim) >= 0


def test_actor_load_dim_with_new_data():
    actor = Actor()
    df = actor.load_dim()
    assert not df.empty


def test_change_dim_actor():
    engine, session_context = get_connection()

    with session_context() as session:
        result = session.query(StgActor).filter_by(bk=-10).first()
        session.delete(result)
        session.commit()

        stg = StgActor(bk=-10, first_name="invalidd", last_name="test")
        session.add(stg)
        session.commit()

        actor = Actor()
        df = actor.load_dim()
        result = df[df['bk'] == -10]
        assert not result.empty
        assert (result['first_name'] == "invalidd").all()
        assert (result['last_name'] == "test").all()

        new_dim = session.query(DimActor).filter_by(bk=-10).first()
        assert new_dim.full_name == "Invalidd Test"

        new_stg = session.query(StgActor).filter_by(bk=-10).first()
        new_stg.first_name = "invalid"
        session.commit()

        del df
        df = actor.load_dim()
        result = df[df['bk'] == -10]
        assert not result.empty
        assert (result['first_name'] == "invalid").all()
        assert (result['last_name'] == "test").all()
        new_dim = session.query(DimActor).filter_by(bk=-10).first()
        assert new_dim.full_name == "Invalid Test"

