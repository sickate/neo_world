from sqlalchemy import Column, Integer, String, Boolean, Date, Float, UniqueConstraint
from sqlbase import Base


# Basic info of stock every day

class DailyBasic(Base):
    __tablename__ = 'daily_basic'
    __table_args__ = (UniqueConstraint('ts_code', 'trade_date', name='_daily_basic_uniq'),)

    id = Column(Integer, primary_key=True)
    ts_code = Column(String(12), index=True)
    trade_date = Column(Date, index=True)
    close = Column(Float(precision=2, asdecimal=True))           # 当日收盘
    turnover_rate = Column(Float(precision=4, asdecimal=True))   # 换手率
    turnover_rate_f = Column(Float(precision=4, asdecimal=True)) # 换手率（自由流通股）
    volume_ratio = Column(Float(precision=2, asdecimal=True))    # 量比
    pe = Column(Float(precision=2, asdecimal=True))              # 市盈率（总市值/净利润）
    pe_ttm = Column(Float(precision=4, asdecimal=True))          # 市盈率（TTM）
    pb = Column(Float(precision=4, asdecimal=True))              # 市净率（总市值/净资产）
    ps = Column(Float(precision=4, asdecimal=True))              # 市销率
    ps_ttm = Column(Float(precision=4, asdecimal=True))          # 市销率（TTM）
    dv_ratio = Column(Float(precision=4, asdecimal=True))        # float 股息率 （%）
    dv_ttm = Column(Float(precision=4, asdecimal=True))          # 股息率（TTM）（%）
    total_share = Column(Float(precision=2, asdecimal=True))     # 总股本 （手）
    float_share = Column(Float(precision=4, asdecimal=True))     # 流通股本 （手）
    free_share = Column(Float(precision=4, asdecimal=True))      # 自由流通股本 （手）
    total_mv = Column(Float(precision=4, asdecimal=True))        # 总市值 （元）
    circ_mv = Column(Float(precision=4, asdecimal=True))         # 流通市值（元）
    free_mv = Column(Float(precision=4, asdecimal=True))         # 自由流通市值（元）
    ma_close_250 = Column(Float(precision=2, asdecimal=True))    # 流通市值（元）

    # only need following
    # turnover_rate
    # trf
    # total_share
    # float_share
    # free_share
    # total_mv
    # circ_mv
    # ma_close_250

    def __init__(self, ts_code, trade_date):
        self.ts_code = ts_code
        self.trade_date = trade_date


    def __repr__(self):
        return '<Stock %r>' % self.ts_code
