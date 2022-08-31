from sqlalchemy import Column, Integer, String, Boolean, Date, Float, UniqueConstraint
from sqlbase import Base

# INDEX_CODES = ['000300.XSHG', '000016.XSHG', '000905.XSHG', '399005.XSHE', '399006.XSHE']
# INDEX_NAMES = ['沪深 300', '上证 50', '中证 500', '创业板', '中小板']
# 代码	名称	最新价	涨跌额	涨跌幅	昨收	今开	最高	最低	成交量	成交额
# ['code', 'name', 'latest', 'amt_chg', 'pct_chg', 'pre_close', 'open', 'high', 'low', 'vol', 'amount']

class Index(Base):
    __tablename__ = 'indices'
    __table_args__ = (UniqueConstraint('ts_code', 'trade_date', name='_indices_uniq'),)

    id = Column(Integer, primary_key=True)
    ts_code = Column(String(12), index=True)
    name = Column(String(12), index=True)
    trade_date = Column(Date, index=True)
    amt_chg = Column(Float(precision=2, asdecimal=True))
    pct_chg = Column(Float(precision=2, asdecimal=True))
    open_ = Column('open', Float(precision=2, asdecimal=True))   # 开盘价
    high = Column(Float(precision=2, asdecimal=True))            # 最高价
    low = Column(Float(precision=2, asdecimal=True))             # 最低价
    close = Column(Float(precision=2, asdecimal=True))           # 当日收盘

    vol = Column(Float(precision=2, asdecimal=True))             # 成交量, 单位股
    amount = Column(Float(precision=2, asdecimal=True))          # 成交额，单位元

    def __init__(self, ts_code, trade_date):
        self.ts_code = ts_code
        self.trade_date = trade_date
