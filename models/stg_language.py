from sqlalchemy import Column, Integer, DateTime, Boolean, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class StgLanguage(Base):
    __tablename__ = "s_language"
    __table_args__ = {"schema": "stg"}

    bk = Column("bk", Integer, primary_key=True)
    name = Column("name", String(20), nullable=False)

    def format_stg_language(self) -> None:
        self.name = self.name.lower()
