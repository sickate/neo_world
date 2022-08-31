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

    # gone in new ts pro
    amp = Column(Float(precision=4, asdecimal=True))           # 振幅
    fc_ratio = Column(Float(precision=4, asdecimal=True))      # 封单金额/日成交金额 百分比n%
    fl_ratio = Column(Float(precision=4, asdecimal=True))      # 封单手数/流通股本 百分比n%
    strth = Column(Float(precision=4, asdecimal=True))         # 涨跌停强度

    fd_amount = Column(Float(precision=4, asdecimal=True))     # 封单金额
    dn_amount = Column(Float(precision=4, asdecimal=True))     # 板上成交金额, only DN

    first_time = Column(String)                                # 首次封板时间, only UP
    last_time = Column(String)                                 # 最后封板时间

    open_times = Column(Integer)                               # 打开次数
    limit = Column(String(1))                                  # D跌停U涨停

    # new in ts pro
    up_stat = Column(String)                                   #涨停统计（N/T T天有N次涨停）
    limit_times = Column(Integer)                              # 连板数
