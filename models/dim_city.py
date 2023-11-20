from sqlalchemy import Column, Integer, DateTime, Boolean, String, Sequence
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class DimCity(Base):
    __tablename__ = "s_city"
    __table_args__ = {"schema": "stg"}

    sk = Column("sk", Integer, Sequence("sk_city", start=1), autoincrement=True, primary_key=True)
    bk = Column("bk", Integer, nullable=False)
    city = Column("city", String(100), nullable=False)
    country_sk = Column("country_sk", Integer, nullable=False)

    def format_city(self) -> None:
        self.city = self.city.capitalize()