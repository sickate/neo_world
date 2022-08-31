from sqlalchemy import Column, Integer, String, Boolean, Date, Float, UniqueConstraint
from sqlbase import Base


class Auction(Base):
    __tablename__ = 'auction'
    __table_args__ = (UniqueConstraint('ts_code', 'trade_date', name='_auctions_ts_code_trade_date_uniq'),)

    id = Column(Integer, primary_key=True)
    ts_code = Column(String(12), index=True)
    trade_date = Column(Date, index=True)

    open_ = Column('open', Float(precision=2, asdecimal=True))    # 开盘价
    pre_close = Column(Float(precision=2, asdecimal=True))        # 前日收盘
    open_pct = Column(Float(precision=2, asdecimal=True))         #
    auc_vol = Column(Float(precision=2, asdecimal=True))          # 成交量, 单位手
    auc_amt = Column(Float(precision=3, asdecimal=True))          # 成交额，单位元


    def __init__(self, ts_code, trade_date):
        self.ts_code = ts_code
        self.trade_date = trade_date


    def __repr__(self):
        return '<Stock %r>' % self.ts_code
