from sqlalchemy import Column, Integer, DateTime, Boolean, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class StgCategory(Base):
    __tablename__ = "s_category"
    __table_args__ = {"schema": "stg"}

    bk = Column("bk", Integer, primary_key=True)
    name = Column("name", String(100), nullable=False)

    def format_stg_category(self) -> None:
        self.name = self.name.lower()
