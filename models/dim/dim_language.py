from sqlalchemy import Column, Integer, DateTime, Boolean, String, Sequence
from models.base_model import BaseModel
from models.stg.stg_language import StgLanguage


class DimLanguage(BaseModel):
    __tablename__ = "language"
    __table_args__ = {"schema": "dim"}

    sk = Column("sk", Integer, Sequence("sk_language", start=1), autoincrement=True, primary_key=True)
    bk = Column("bk", Integer, nullable=False)
    name = Column("name", String(20), nullable=False)

    def format_language(self):
        self.name = self.name.capitalize()
