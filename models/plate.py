from sqlalchemy import Column, Integer, String, Boolean, Date, Float, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from sqlbase import Base


class Plate(Base):
    __tablename__ = 'plates'
    __table_args__ = (UniqueConstraint('plate_type', 'name', 'trade_date'),)

    id = Column(Integer, primary_key=True)
    plate_type = Column(String(8))
    name = Column(String(16), index=True)
    trade_date = Column(Date, index=True)

    upstop_stocks = Column(ARRAY(String))
    top_stocks = Column(ARRAY(String))

    daily_rank = Column(Integer)
    pct_chg = Column(Float(precision=4, asdecimal=True))         # 涨跌幅
    amount = Column(Float(precision=3, asdecimal=True))          # 成交额，单位千元
    upstop_num = Column(Integer)
    up = Column(Integer)
    dn = Column(Integer)
    fl = Column(Integer)

    def __init__(self, name, trade_date, plate_type):
        self.name = name
        self.trade_date = trade_date
        self.plate_type = plate_type


    def __repr__(self):
        return '<Stock %r>' % self.ts_code
