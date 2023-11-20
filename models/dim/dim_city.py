from sqlalchemy import Column, Integer, DateTime, Boolean, String, Sequence
from models.base_model import BaseModel


class DimCity(BaseModel):
    __tablename__ = "city"
    __table_args__ = {"schema": "dim"}

    sk = Column("sk", Integer, Sequence("sk_city", start=1), autoincrement=True, primary_key=True)
    bk = Column("bk", Integer, nullable=False)
    city = Column("city", String(100), nullable=False)
    country_sk = Column("country_sk", Integer, nullable=False)

    def format_city(self) -> None:
        self.city = self.city.capitalize()