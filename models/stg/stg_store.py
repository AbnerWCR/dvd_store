from sqlalchemy import Column, Integer, DateTime, Boolean, String
from models.base_model import BaseModel


class StgStore(BaseModel):
    __tablename__ = "s_store"
    __table_args__ = {"schema": "stg"}

    bk = Column("bk", Integer, primary_key=True)
    manager = Column("manager", String(100), nullable=False)
    address_bk = Column("address_bk", Integer, nullable=False)

    def format_stg_store(self) -> None:
        self.manager = self.manager.lower()
