from models.dim_actor import DimActor
from models.stg_actor import StgActor


def test_create_obj_dim_actor():
    dim_actor = DimActor(sk=1,
                         bk=1,
                         full_name="Abner Rodrigues")
    assert dim_actor is not None


def test_create_dim_from_stg():
    stg = StgActor(bk=1, first_name="Wallace", last_name="Costa")
    dim = DimActor()
    dim.create_dim_from_stg(stg)
    assert dim is not None
