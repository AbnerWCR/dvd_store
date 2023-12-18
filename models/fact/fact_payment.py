from sqlalchemy import Column, BigInteger, Integer, Float, Boolean, DateTime, PrimaryKeyConstraint, String, Sequence
from models.base_model import BaseModel


class FactPayment(BaseModel):
    __tablename__ = "payment"
    __table_args__ = {"schema": "fact"}

    sk = Column("sk", BigInteger, autoincrement=False, primary_key=True)
    bk = Column("bk", Integer, nullable=False)
    payment_date = Column("payment_date", DateTime)
    sk_payment_date = Column("sk_payment_date", Integer)
    customer_sk = Column("customer_sk", Integer)
    staff = Column("staff", String(250))
    amount = Column("amount", Float)
    rental_staff = Column("rental_staff", String(250))
    rental_date = Column("rental_date", DateTime)
    sk_rental_date = Column("sk_rental_date", Integer)
    return_date = Column("return_date", DateTime)
    sk_return_date = Column("sk_return_date", Integer)
