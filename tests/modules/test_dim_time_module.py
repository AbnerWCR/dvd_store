from modules.dim_time import create_dim_time


def test_dim_time_module():
    dim_time = create_dim_time()
    assert dim_time is not None
