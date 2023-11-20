from sqlalchemy import Column, Integer, DateTime, Boolean, String, SmallInteger, Float, Double, DECIMAL
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class StgFilm(Base):
    __tablename__ = "s_film"
    __table_args__ = {"schema": "stg"}

    bk = Column("bk", Integer, primary_key=True)
    title = Column("title", String(100), nullable=False)
    release_year = Column("release_year", Integer, nullable=False)
    language_bk = Column("language_bk", Integer, nullable=False)
    original_language_bk = Column("original_language_bk", Integer, nullable=False)
    rental_duration = Column("rental_duration", SmallInteger, nullable=False)
    rental_rate = Column("rental_rate", DECIMAL(4, 2), nullable=False)
    length = Column("length", SmallInteger, nullable=False)
    replacement_cost = Column("replacement_cost", DECIMAL(5, 2), nullable=False)
    rating = Column("rating", String(25))
    actor_bk = Column("actor_bk", Integer, nullable=False)
    category_bk = Column("category_bk", Integer, nullable=False)

    def format_stg_store(self) -> None:
        self.title = self.title.lower()
        self.rating = self.rating.upper()
