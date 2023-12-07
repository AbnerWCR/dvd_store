from sqlalchemy import Column, Integer, DateTime, Boolean, String, SmallInteger, Float, Double, DECIMAL
from models.base_model import BaseModel


class StgFilm(BaseModel):
    __tablename__ = "s_film"
    __table_args__ = {"schema": "stg"}

    bk = Column("bk", Integer, primary_key=True)
    title = Column("title", String(100), nullable=False)
    release_year = Column("release_year", Integer, nullable=False)
    language_bk = Column("language_bk", Integer, nullable=False)
    original_language_bk = Column("original_language_bk", Integer, nullable=True)
    rental_duration = Column("rental_duration", SmallInteger, nullable=False)
    rental_rate = Column("rental_rate", DECIMAL(4, 2), nullable=False)
    length = Column("length", SmallInteger, nullable=False)
    replacement_cost = Column("replacement_cost", DECIMAL(5, 2), nullable=False)
    rating = Column("rating", String(25))
    actor_bk = Column("actor_bk", Integer, nullable=False)
    category_bk = Column("category_bk", Integer, nullable=False)

    def __init__(self, bk, title, release_year, language_bk, original_language_bk, rental_duration, rental_rate, length, replacement_cost, rating, actor_bk, category_bk):
        self.bk = bk
        self.title = title.lower()
        self.release_year = release_year
        self.language_bk = language_bk
        self.original_language_bk = original_language_bk
        self.rental_duration = rental_duration
        self.rental_rate = rental_rate
        self.length = length
        self.replacement_cost = replacement_cost
        self.rating = rating.upper()
        self.actor_bk = actor_bk
        self.category_bk = category_bk

    def format_stg_film(self) -> None:
        self.title = self.title.lower()
        self.rating = self.rating.upper()
