from sqlalchemy import Column, Integer, DateTime, Boolean, String
from models.base_model import BaseModel


class StgCostumer(BaseModel):
    __tablename__ = "s_costumer"
    __table_args__ = {"schema": "stg"}

    bk = Column("bk", Integer, primary_key=True)
    first_name = Column("first_name", String(50), nullable=False)
    last_name = Column("last_name", String(50), nullable=False)
    email = Column("email", String(150), nullable=False)
    address_bk = Column("address_bk", Integer, nullable=False)
    active = Column("active", Boolean)

    def format_stg_costumer(self) -> None:
        self.first_name = self.first_name.lower()
        self.last_name = self.last_name.lower()
        self.email = self.email.lower()
