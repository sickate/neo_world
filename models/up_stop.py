from sqlalchemy import Column, Integer, String, Boolean, Date, Float, UniqueConstraint
from sqlalchemy.dialects import postgresql
from sqlbase import Base


class UpStop(Base):
    """
    """
    __tablename__ = 'upstop'
    __table_args__ = (UniqueConstraint('ts_code', 'trade_date', name='_upstop_uniq'),)


    id = Column(Integer, primary_key=True)
    ts_code = Column(String(12), index=True)
    trade_date = Column(Date, index=True)
    name = Column(String(10), index=True)
    close = Column(Float(precision=2, asdecimal=True))         # 当日收盘
    pct_chg = Column(Float(precision=4, asdecimal=True))       # 涨跌幅
    amp = Column(Float(precision=4, asdecimal=True))           # 振幅
    fc_ratio = Column(Float(precision=4, asdecimal=True))      # 封单金额/日成交金额 百分比n%
    fl_ratio = Column(Float(precision=4, asdecimal=True))      # 封单手数/流通股本 百分比n%
    fd_amount = Column(Float(precision=4, asdecimal=True))     # 封单金额
    first_time = Column(String)                                # 首次涨停时间
    last_time = Column(String)                                 # 最后封板时间
    open_times = Column(Integer)                               # 打开次数
    strth = Column(Float(precision=4, asdecimal=True))         # 涨跌停强度
    limit = Column(String(1))                                  # D跌停U涨停
