from sqlalchemy import Column, Integer, DateTime, Boolean, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class StgActor(Base):
    __tablename__ = "s_actor"
    __table_args__ = {"schema": "stg"}

    bk_actor = Column("bk_actor", Integer, primary_key=True)
    first_name = Column("first_name", String(50))
    last_name = Column("last_name", String(50))
