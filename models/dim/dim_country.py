from sqlalchemy import Column, Integer, DateTime, Boolean, String, Sequence
from models.base_model import BaseModel
from models.stg.stg_country import StgCountry


class DimCountry(BaseModel):
    __tablename__ = "country"
    __table_args__ = {"schema": "dim"}

    sk = Column("sk", Integer, Sequence("sk_country", start=1), autoincrement=True, primary_key=True)
    bk = Column("bk", Integer, nullable=False)
    country = Column("country", String(100), nullable=False)

    def create_dim_from_stg(self, stg: StgCountry):
        self.bk = stg.bk
        self.country = stg.country.capitalize()
