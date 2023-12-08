from sqlalchemy import Column, Integer, DateTime, Boolean, String, Sequence
from models.base_model import BaseModel


class DimCustomer(BaseModel):
    __tablename__ = "customer"
    __table_args__ = {"schema": "dim"}

    sk = Column("sk", Integer, Sequence("sk_costumer", start=1), autoincrement=True, primary_key=True)
    bk = Column("bk", Integer, nullable=False)
    full_name = Column("full_name", String(101), nullable=False)
    email = Column("email", String(150), nullable=False)
    address_sk = Column("address_sk", Integer, nullable=False)
    active = Column("active", Boolean)
    store_sk = Column("store_sk", Integer, nullable=False)

    def set_full_name(self, first_name: str, last_name: str) -> None:
        self.full_name = f"{first_name.capitalize()} {last_name.capitalize()}"
