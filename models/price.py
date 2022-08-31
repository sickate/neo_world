from sqlalchemy import Column, Integer, String, Boolean, Date, Float, UniqueConstraint
from sqlbase import Base


class Price(Base):
    __tablename__ = 'price'
    __table_args__ = (UniqueConstraint('ts_code', 'trade_date', name='_prices_uniq'),)

    id = Column(Integer, primary_key=True)
    ts_code = Column(String(12), index=True)
    trade_date = Column(Date, index=True)
    open_ = Column('open', Float(precision=2, asdecimal=True))   # 开盘价
    high = Column(Float(precision=2, asdecimal=True))            # 最高价
    low = Column(Float(precision=2, asdecimal=True))             # 最低价
    close = Column(Float(precision=2, asdecimal=True))           # 当日收盘
    pre_close = Column(Float(precision=2, asdecimal=True))       # 前日收盘
    change = Column(Float(precision=2, asdecimal=True))          # 价格变动
    pct_chg = Column(Float(precision=4, asdecimal=True))         # 涨跌幅
    vol = Column(Float(precision=2, asdecimal=True))             # 成交量, 单位手
    amount = Column(Float(precision=3, asdecimal=True))          # 成交额，单位元

    def __init__(self, ts_code, trade_date, open_, high, low, close, pre_close, change, pct_chg, vol, amount):
        self.ts_code = ts_code
        self.trade_date = trade_date
        self.open_ = open_
        self.high = high
        self.low = low
        self.close = close
        self.pre_close = pre_close
        self.change = change
        self.pct_chg = pct_chg
        self.vol = vol
        self.amount = amount


    def __repr__(self):
        return '<Stock %r>' % self.ts_code
