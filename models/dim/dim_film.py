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

    def format_film(self) -> None:
        self.title = self.title.capitalize()
