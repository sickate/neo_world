from enum import Enum
from sqlalchemy import Column, Integer, String, Boolean, Date, Float, UniqueConstraint
from sqlalchemy.dialects import postgresql
from sqlbase import Base


class TradeNote(Base):
    """
    """

    __tablename__ = 'notes'
    __table_args__ = (UniqueConstraint('ts_code', 'trade_date', 'action', name='_notes_uniq'),)

    id = Column(Integer, primary_key=True)

    ts_code = Column(String(12), index=True)
    trade_date = Column(Date, index=True)

    action = Column(String) # watch/positive/negtive/buy/sell
    share = Column(Integer)
    price = Column(Float(precision=3, asdecimal=True)) # compare with high, low, open, close
    fee  = Column(Float(precision=2, asdecimal=True))
    reason = Column(String)

    # next 3,5,10
    def price_after(days=1):
        pass


    # prev 3,5,10
    def price_before(days=1):
        pass


    def __init__(self, ts_code, trade_date, action='watch'):
        self.ts_code = ts_code
        self.trade_date = trade_date
        self.action = action

