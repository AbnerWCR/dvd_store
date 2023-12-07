from sqlalchemy import Column, Integer, DateTime, Boolean, String, SmallInteger, Sequence, DECIMAL
from models.base_model import BaseModel


class DimFilm(BaseModel):
    __tablename__ = "film"
    __table_args__ = {"schema": "dim"}

    sk = Column("sk", Integer, Sequence("sk_film", start=1), autoincrement=True, primary_key=True)
    bk = Column("bk", Integer, nullable=False)
    title = Column("title", String(100), nullable=False)
    release_year = Column("release_year", Integer, nullable=False)
    language_sk = Column("language_sk", Integer, nullable=False)
    original_language_sk = Column("original_language_sk", Integer, nullable=False)
    rental_duration = Column("rental_duration", SmallInteger, nullable=False)
    rental_rate = Column("rental_rate", DECIMAL(4, 2), nullable=False)
    length = Column("length", SmallInteger, nullable=False)
    replacement_cost = Column("replacement_cost", DECIMAL(5, 2), nullable=False)
    rating = Column("rating", String(25))
    actor_sk = Column("actor_sk", Integer, nullable=False)
    category_sk = Column("category_sk", Integer, nullable=False)

    def __init__(self, bk, title, release_year, language_sk, original_language_sk, rental_duration,
                 rental_rate, length, replacement_cost, actor_sk, category_sk, rating=None):
        self.bk = bk
        self.title = title.capitalize()
        self.release_year = release_year
        self.language_sk = language_sk
        self.original_language_sk = original_language_sk
        self.rental_duration = rental_duration
        self.rental_rate = rental_rate
        self.length = length
        self.replacement_cost = replacement_cost
        self.actor_sk = actor_sk
        self.category_sk = category_sk
        self.rating = rating

    def format_film(self) -> None:
        self.title = self.title.capitalize()
