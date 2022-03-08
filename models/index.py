from sqlalchemy import Column, Integer, String, Boolean, Date, Float, UniqueConstraint
from sqlbase import Base

INDEX_CODES = ['000300.XSHG', '000016.XSHG', '000905.XSHG', '399005.XSHE', '399006.XSHE']
INDEX_NAMES = ['沪深 300', '上证 50', '中证 500', '创业板', '中小板']


class Index(Base):
    __tablename__ = 'indices'
    __table_args__ = (UniqueConstraint('ts_code', 'trade_date', name='_indices_uniq'),)

    id = Column(Integer, primary_key=True)
    ts_code = Column(String(12), index=True)
    trade_date = Column(Date, index=True)
    val = Column(Float(precision=2, asdecimal=True))
    pct_chg = Column(Float(precision=2, asdecimal=True))
    net_mf_amount = Column(Float(precision=2, asdecimal=True)) # 单位万元
    net_main_amount = Column(Float(precision=2, asdecimal=True))   # 单位万元

    def __init__(self, ts_code, trade_date):
        self.ts_code = ts_code
        self.trade_date = trade_date
