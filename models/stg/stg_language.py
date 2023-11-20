from sqlalchemy import Column, Integer, DateTime, Boolean, String
from models.base_model import BaseModel


class StgLanguage(BaseModel):
    __tablename__ = "s_language"
    __table_args__ = {"schema": "stg"}

    bk = Column("bk", Integer, primary_key=True)
    name = Column("name", String(20), nullable=False)

    def format_stg_language(self) -> None:
        self.name = self.name.lower()
