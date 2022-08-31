from sqlalchemy import Column, Integer, String, Boolean, Date, Float, UniqueConstraint
from sqlalchemy.dialects import postgresql
from sqlbase import Base


class LimitStock(Base):
    """
    """
    __tablename__ = 'limit_stocks'
    __table_args__ = (UniqueConstraint('ts_code', 'trade_date', name='_limit_stocks_uniq'),)


    id = Column(Integer, primary_key=True)
    ts_code = Column(String(12), index=True)
    trade_date = Column(Date, index=True)

    name = Column(String(10), index=True)
    close = Column(Float(precision=2, asdecimal=True))         # 当日收盘
    pct_chg = Column(Float(precision=4, asdecimal=True))       # 涨跌幅

    amount = Column(Float(precision=4, asdecimal=True))        # 成交金额, 单位元
    fd_amount = Column(Float(precision=4, asdecimal=True))     # 封单金额, 单位元
    dn_amount = Column(Float(precision=4, asdecimal=True))     # 板上成交金额, only DN

    first_time = Column(String)                                # 首次封板时间, only UP
    last_time = Column(String)                                 # 最后封板时间
    open_times = Column(Integer)                               # 打开次数

    up_stat = Column(String)                                   # 涨停统计（N/T T天有N次涨停）
    conseq_up_num = Column(Integer)                            # 连UP板数
    conseq_dn_num = Column(Integer)                            # 连DN板数

    limit = Column(String(1))                                  # D跌停U涨停Z

    industry = Column(String(12))                              # 行业
