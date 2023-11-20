from sqlalchemy import Column, Integer, DateTime, Boolean, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class StgActor(Base):
    __tablename__ = "s_actor"
    __table_args__ = {"schema": "stg"}

    bk = Column("bk", Integer, primary_key=True)
    first_name = Column("first_name", String(50))
    last_name = Column("last_name", String(50))

    def format_stg(self) -> None:
        if self.first_name is not None:
            self.first_name = self.first_name.lower()

        if self.last_name is not None:
            self.last_name = self.last_name.lower()
