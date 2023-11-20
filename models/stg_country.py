from sqlalchemy import Column, Integer, DateTime, Boolean, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class StgCountry(Base):
    __tablename__ = "s_country"
    __table_args__ = {"schema": "stg"}

    bk = Column("bk", Integer, primary_key=True)
    country = Column("country", String(100), nullable=False)

    def format_stg_country(self) -> None:
        self.country = self.country.lower()
