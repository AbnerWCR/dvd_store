from sqlalchemy import Column, Integer, DateTime, Boolean, String, Sequence
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class DimStore(Base):
    __tablename__ = "store"
    __table_args__ = {"schema": "dim"}

    sk = Column("sk", Integer, Sequence("sk_store", start=1), autoincrement=True, primary_key=True)
    bk = Column("bk", Integer, nullable=False)
    manager = Column("manager", String(100), nullable=False)
    address_bk = Column("address_bk", Integer, nullable=False)

    def format_store(self) -> None:
        self.manager = self.manager.capitalize()
