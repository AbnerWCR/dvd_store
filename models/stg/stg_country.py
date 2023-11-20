from sqlalchemy import Column, Integer, DateTime, Boolean, String
from models.base_model import BaseModel


class StgCountry(BaseModel):
    __tablename__ = "s_country"
    __table_args__ = {"schema": "stg"}

    bk = Column("bk", Integer, primary_key=True)
    country = Column("country", String(100), nullable=False)

    def format_stg_country(self) -> None:
        self.country = self.country.lower()
