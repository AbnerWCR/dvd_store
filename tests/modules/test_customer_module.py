from modules.customer import Customer
from infra.db_connection import get_connection
from models.stg.stg_customer import StgCustomer
from models.dim.dim_customer import DimCustomer


def test_customer_load_stg():
    customer = Customer()
    df = customer.load_stg()
    assert not df.empty


def test_store_load_dim_from_db():
    customer = Customer()
    dim = customer.load_dim_from_db()
    assert len(dim) >= 0


def test_store_load_dim_with_new_data():
    customer = Customer()
    df = customer.load_dim()
    assert not df.empty


def test_change_dim_store():
    engine, session_context = get_connection()

    with session_context() as session:
        result = session.query(StgCustomer).filter_by(bk=-10).first()

        if result is not None:
            session.delete(result)
            session.commit()

        stg = StgCustomer(bk=-10,
                          first_name="invalidd",
                          last_name="valUe",
                          email="invalid.email@test.com",
                          address_bk=-10,
                          active=0,
                          store_bk=-10)
        stg.format_stg_costumer()
        session.add(stg)
        session.commit()

        customer = Customer()
        df = customer.load_dim()
        result = df[df['bk'] == -10]
        assert not result.empty
        assert (result['first_name'] == "invalidd").all()
        assert (result['last_name'] == "value").all()
        assert (result['email'] == "invalid.email@test.com").all()
        assert (result['address_bk'] == -10).all()
        assert (result['active'] == 0).all()
        assert (result['store_bk'] == -10).all()

        new_dim = session.query(DimCustomer).filter_by(bk=-10).first()
        assert new_dim.full_name == "Invalidd Value"
        assert new_dim.email == "invalid.email@test.com"
        assert new_dim.address_sk == -1
        assert new_dim.store_sk == -1
        assert new_dim.active == 0

        new_stg = session.query(StgCustomer).filter_by(bk=-10).first()
        new_stg.first_name = "invalid"
        new_stg.last_name = "valUE"
        new_stg.format_stg_costumer()
        session.commit()

        del df
        df = customer.load_dim()
        result = df[df['bk'] == -10]
        assert not result.empty
        assert (result['first_name'] == "invalid").all()
        assert (result['last_name'] == " value").all()
        new_dim = session.query(DimCustomer).filter_by(bk=-10).first()
        assert new_dim.full_name == "Invalid Value"
        assert new_dim.address_sk == -1

        session.delete(new_dim)
        session.commit()
