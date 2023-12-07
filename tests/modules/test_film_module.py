from modules.film import Film
from infra.db_connection import get_connection
from models.stg.stg_film import StgFilm
from models.dim.dim_film import DimFilm


def test_film_load_stg():
    film = Film()
    df = film.load_stg()
    assert not df.empty


def test_film_load_dim_from_db():
    film = Film()
    dim = film.load_dim_from_db()
    assert len(dim) >= 0


def test_film_load_dim_with_new_data():
    film = Film()
    df = film.load_dim()
    assert not df.empty


def test_change_dim_country():
    engine, session_context = get_connection()

    with session_context() as session:
        result = session.query(StgFilm).filter_by(bk=-10).first()

        if result is not None:
            session.delete(result)
            session.commit()

        stg = StgFilm(bk=-10,
                      title="invalidd",
                      release_year=-10,
                      language_bk=-10,
                      original_language_bk=-10,
                      rental_duration=-1,
                      rental_rate=-10.00,
                      length=-1,
                      replacement_cost=-10.00,
                      actor_bk=-10,
                      category_bk=-10,
                      rating='-10'
                      )
        session.add(stg)
        session.commit()

        film = Film()
        df = film.load_dim()
        result = df[df['bk'] == -10]
        assert not result.empty
        assert (result['title'] == "invalidd").all()
        assert (result['release_year'] == -10).all()
        assert (result['language_bk'] == -10).all()
        assert (result['original_language_bk'] == -10).all()
        assert (result['rental_duration'] == -1).all()
        assert (result['rental_rate'] == -10.00).all()
        assert (result['length'] == -1).all()
        assert (result['replacement_cost'] == -10.00).all()
        assert (result['actor_bk'] == -10).all()
        assert (result['category_bk'] == -10).all()
        assert (result['rating'] == '-10').all()

        new_dim = session.query(DimFilm).filter_by(bk=-10).first()
        assert new_dim.title == "Invalidd"
        assert new_dim.release_year == -10
        assert new_dim.language_sk == -1
        assert new_dim.original_language_sk == -1
        assert new_dim.rental_duration == -1
        assert new_dim.rental_rate == -10.00
        assert new_dim.length == -1
        assert new_dim.replacement_cost == -10.00
        assert new_dim.actor_sk == -1
        assert new_dim.category_sk == -1
        assert new_dim.rating == '-10'

        new_stg = session.query(StgFilm).filter_by(bk=-10).first()
        new_stg.title = "invalid"
        new_stg.rating = '-100'
        session.commit()

        del df
        df = film.load_dim()
        result = df[df['bk'] == -10]
        assert not result.empty
        assert (result['title'] == "invalid").all()
        assert (result['release_year'] == -10).all()
        assert (result['language_bk'] == -10).all()
        assert (result['original_language_bk'] == -10).all()
        assert (result['rental_duration'] == -1).all()
        assert (result['rental_rate'] == -10.00).all()
        assert (result['length'] == -1).all()
        assert (result['replacement_cost'] == -10.00).all()
        assert (result['actor_bk'] == -10).all()
        assert (result['category_bk'] == -10).all()
        assert (result['rating'] == '-100').all()
        new_dim = session.query(DimFilm).filter_by(bk=-10).first()
        assert new_dim.title == "Invalid"
        assert new_dim.release_year == -10
        assert new_dim.language_sk == -1
        assert new_dim.original_language_sk == -1
        assert new_dim.rental_duration == -1
        assert new_dim.rental_rate == -10.00
        assert new_dim.length == -1
        assert new_dim.replacement_cost == -10.00
        assert new_dim.actor_sk == -1
        assert new_dim.category_sk == -1
        assert new_dim.rating == '-100'

        session.delete(new_dim)
        session.commit()
