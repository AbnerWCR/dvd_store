from modules.address import Address
from infra.db_connection import get_connection
from models.stg.stg_address import StgAddress
from models.dim.dim_address import DimAddress


def test_address_load_stg():
    address = Address()
    df = address.load_stg()
    assert df is not None


def test_address_load_dim_from_db():
    address = Address()
    dim = address.load_dim_from_db()
    assert len(dim) >= 0


def test_address_load_dim_with_new_data():
    address = Address()
    df = address.load_dim()
    assert df is not None


def test_change_dim_address():
    engine, session_context = get_connection()

    with session_context() as session:
        result = session.query(StgAddress).filter_by(bk=-10).first()

        if result is not None:
            session.delete(result)
            session.commit()

        stg = StgAddress(bk=-10, address="invalidd", country_bk=-10, distirct="invallid")
        session.add(stg)
        session.commit()

        address = Address()
        df = address.load_dim()
        result = df[df['bk'] == -10]
        assert not result.empty
        assert (result['address'] == "invalidd").all()
        assert (result['district'] == "invallid").all()

        new_dim = session.query(DimAddress).filter_by(bk=-10).first()
        assert new_dim.address == "Invalidd"
        assert new_dim.district == "Invalidd"

        new_stg = session.query(StgAddress).filter_by(bk=-10).first()
        new_stg.address = "invalid"
        new_stg.address2 = "invalid2"
        new_stg.district = "invalid"
        session.commit()

        del df
        df = address.load_dim()
        result = df[df['bk'] == -10]
        assert not result.empty
        assert (result['address'] == "invalid").all()
        assert (result['address2'] == "invalid2").all()
        assert (result['district'] == "invalid").all()
        new_dim = session.query(DimAddress).filter_by(bk=-10).first()
        assert new_dim.address == "Invalid"
        assert new_dim.address2 == "Invalid2"
        assert new_dim.district == "Invalid"

        session.delete(new_dim)
        session.commit()
