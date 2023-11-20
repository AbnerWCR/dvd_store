from sqlalchemy import Column, Integer, DateTime, Boolean, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class StgAddress(Base):
    __tablename__ = "s_address"
    __table_args__ = {"schema": "stg"}

    bk = Column("bk", Integer, primary_key=True)
    address = Column("address", String(150), nullable=False)
    address2 = Column("address2", String(150), nullable=True)
    district = Column("district", String(150), nullable=False)
    city_bk = Column("city_bk", Integer, nullable=False)

    def format_stg_address(self) -> None:
        self.address = self.address.lower()
        self.address2 = self.address2.lower() if self.address2 is not None else None
        self.district = self.district.lower()
