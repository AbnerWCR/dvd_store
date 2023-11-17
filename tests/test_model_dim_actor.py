from models.dim_actor import DimActor

def test_create_obj_dim_actor():
    try:
        dim_actor = DimActor("abner", "wallace")
        assert dim_actor is not None
    except:
        assert False