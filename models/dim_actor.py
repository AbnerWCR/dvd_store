from sqlalchemy import Column, Integer, Boolean, DateTime, PrimaryKeyConstraint, String, Sequence
from sqlalchemy.ext.declarative import declarative_base
from models.stg_actor import StgActor

Base = declarative_base()


class DimActor(Base):
    __tablename__ = "actor"
    __table_args__ = {"schema": "dim"}

    sk = Column("sk", Integer, Sequence("sk_actor", start=1), primary_key=True, autoincrement=True)
    bk = Column("bk", Integer, nullable=False)
    full_name = Column("full_name", String(101), nullable=False)

    def create_dim_from_stg(self, stg: StgActor) -> None:
        self.bk = stg.bk
        first_name = stg.first_name.capitalize() if stg.first_name is not None else None
        last_name = stg.last_name.capitalize() if stg.last_name is not None else None
        self.full_name = f"{first_name} {last_name}"
