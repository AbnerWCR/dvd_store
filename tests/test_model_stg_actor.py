from models.stg_actor import StgActor


def test_stg_actor_create():
    try:
        stg_actor = StgActor(bk=1,
                             first_name="abner",
                             last_name="wallace")
        assert stg_actor is not None
    except:
        assert False


def test_format_stg_actor():
    stg_actor = StgActor(bk=1,
                         first_name="abner",
                         last_name="wallace")
    stg_actor.format_stg_actor()
    assert stg_actor is not None
