from sqlalchemy import Column, Integer, DateTime, Boolean, String
from models.base_model import BaseModel


class StgCity(BaseModel):
    __tablename__ = "s_city"
    __table_args__ = {"schema": "stg"}

    bk = Column("bk", Integer, primary_key=True)
    city = Column("city", String(100), nullable=False)
    country_bk = Column("country_bk", Integer, nullable=False)

    def format_stg_city(self) -> None:
        self.city = self.city.lower()
