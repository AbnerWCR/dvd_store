from sqlalchemy import Column, Integer, DateTime, Boolean, String, Sequence
from models.base_model import BaseModel
from models.stg.stg_category import StgCategory


class DimCategory(BaseModel):
    __tablename__ = "category"
    __table_args__ = {"schema": "dim"}

    sk = Column("sk", Integer, Sequence("sk_language", start=1), autoincrement=True, primary_key=True)
    bk = Column("bk", Integer, nullable=False)
    name = Column("name", String(100), nullable=False)

    def create_dim_from_stg(self, stg: StgCategory) -> None:
        self.bk = stg.bk
        self.name = stg.name.capitalize()
