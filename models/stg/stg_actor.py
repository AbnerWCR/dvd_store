from sqlalchemy import Column, Integer, DateTime, Boolean, String
from models.base_model import BaseModel


class StgActor(BaseModel):
    __tablename__ = "s_actor"
    __table_args__ = {"schema": "stg"}

    bk = Column("bk", Integer, primary_key=True)
    first_name = Column("first_name", String(50), nullable=False)
    last_name = Column("last_name", String(50), nullable=False)

    def format_stg_actor(self) -> None:
        if self.first_name is not None:
            self.first_name = self.first_name.lower()

        if self.last_name is not None:
            self.last_name = self.last_name.lower()
