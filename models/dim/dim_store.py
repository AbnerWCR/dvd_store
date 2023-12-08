from sqlalchemy import Column, Integer, DateTime, Boolean, String, Sequence
from models.base_model import BaseModel


class DimStore(BaseModel):
    __tablename__ = "store"
    __table_args__ = {"schema": "dim"}

    sk = Column("sk", Integer, Sequence("sk_store", start=1), autoincrement=True, primary_key=True)
    bk = Column("bk", Integer, nullable=False)
    manager = Column("manager", String(100), nullable=False)
    address_sk = Column("address_sk", Integer, nullable=False)

    def format_store(self) -> None:
        temp_manager = self.manager.split(" ")
        self.manager = f"{temp_manager[0].capitalize()} {temp_manager[1].capitalize()}"
