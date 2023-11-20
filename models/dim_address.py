from sqlalchemy import Column, Integer, DateTime, Boolean, String, Sequence
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class DimAddress(Base):
    __tablename__ = "address"
    __table_args__ = {"schema": "dim"}

    sk = Column("sk", Integer, Sequence("sk_address", start=1), autoincrement=True, primary_key=True)
    bk = Column("bk", Integer, nullable=False)
    address = Column("address", String(150), nullable=False)
    address2 = Column("address2", String(150), nullable=True)
    district = Column("district", String(150), nullable=False)
    city_sk = Column("city_sk", Integer, nullable=False)

    def format_address(self) -> None:
        self.address = self.address.capitalize()
        self.address2 = self.address2.capitalize() if self.address2 is not None else None
        self.district = self.district.capitalize()
