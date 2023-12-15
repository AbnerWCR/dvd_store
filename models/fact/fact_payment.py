from sqlalchemy import Column, BigInteger, Integer, Float, Boolean, DateTime, PrimaryKeyConstraint, String, Sequence
from models.base_model import BaseModel


class FactPayment(BaseModel):
    __tablename__ = "payment"
    __table_args__ = {"schema": "fact"}

    sk = Column("sk", BigInteger, autoincrement=False, primary_key=True)
    bk = Column("bk", Integer, nullable=False)
    payment_date = Column("payment_date", DateTime)
    customer_sk = Column("customer_sk", Integer)
    staff = Column("staff", String(250))
    amount = Column("amount", Float)
    rental_staff = Column("rental_staff", String(250))
    rental_date = Column("rental_date", DateTime)
    return_date = Column("return_date", DateTime)
