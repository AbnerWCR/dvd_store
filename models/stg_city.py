from sqlalchemy import Column, Integer, DateTime, Boolean, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class StgCity(Base):
    __tablename__ = "s_city"
    __table_args__ = {"schema": "stg"}

    bk = Column("bk", Integer, primary_key=True)
    city = Column("city", String(100), nullable=False)
    country_bk = Column("country_bk", Integer, nullable=False)

    def format_stg_city(self) -> None:
        self.city = self.city.lower()
