from sqlalchemy import Column, Integer, DateTime, Boolean, String, Sequence
from sqlalchemy.ext.declarative import declarative_base
from models.stg_country import StgCountry

Base = declarative_base()


class DimCountry(Base):
    __tablename__ = "country"
    __table_args__ = {"schema": "dim"}

    sk = Column("sk", Integer, Sequence("sk_country", start=1), autoincrement=True, primary_key=True)
    bk = Column("bk", Integer, nullable=False)
    country = Column("country", String(100), nullable=False)

    def create_dim_from_stg(self, stg: StgCountry):
        self.bk = stg.bk
        self.country = stg.country.capitalize()
