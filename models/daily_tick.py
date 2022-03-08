from sqlalchemy import Column, Integer, String, Boolean, Date, Float, UniqueConstraint
from sqlbase import Base


class DailyTick(Base):
    __tablename__ = 'daily_tick'
    __table_args__ = (UniqueConstraint('ts_code', 'trade_date', name='_daily_tick_uniq'),)

    id = Column(Integer, primary_key=True)
    ts_code = Column(String(12), index=True)
    trade_date = Column(Date, index=True)

    # basic
    # TODO: FIX ADJ
    open_ = Column('open', Float(precision=2, asdecimal=True))   # 开盘价
    high = Column(Float(precision=2, asdecimal=True))            # 最高价
    low = Column(Float(precision=2, asdecimal=True))             # 最低价
    close = Column(Float(precision=2, asdecimal=True))           # 当日收盘
    adj_vol = Column(Float(precision=2, asdecimal=True))         # adj 总量

    # pct_chg
    pct_chg_p1 = Column(Float(precision=4, asdecimal=True))   # 2日累积涨幅
    pct_chg_p2 = Column(Float(precision=4, asdecimal=True))   # 3日累积涨幅
    pct_chg_p4 = Column(Float(precision=4, asdecimal=True))   # 5日累积涨幅
    pct_chg_p9 = Column(Float(precision=4, asdecimal=True))   # 10日累积涨幅

    # this column is not expensive, can be calculated on the fly
    next_pct_chg = Column(Float(precision=4, asdecimal=True))   # 未来1日涨幅

    next2_pct_chg_p1 = Column(Float(precision=4, asdecimal=True))   # 未来2日累积涨幅
    next3_pct_chg_p2 = Column(Float(precision=4, asdecimal=True))   # 未来3日累积涨幅
    next5_pct_chg_p4 = Column(Float(precision=4, asdecimal=True))   # 未来5日累积涨幅
    next10_pct_chg_p9 = Column(Float(precision=4, asdecimal=True))   # 未来10日累积涨幅

    # open pct
    open_pct = Column(Float(precision=4, asdecimal=True))   # 未来1日开盘涨幅
    close_v_open =  Column(Float(precision=4, asdecimal=True))   # 收盘相对开盘涨幅
    next_open_pct = Column(Float(precision=4, asdecimal=True))   # 未来1日开盘涨幅
    next_cvo = Column(Float(precision=4, asdecimal=True))   # 未来1日收盘相对开盘涨幅

    # price, max/min
    max_high_p59 = Column(Float(precision=2, asdecimal=True))   # 过去 60 天最高价
    min_low_p59 = Column(Float(precision=2, asdecimal=True))   # 过去 60 天最高价

    # ma close
    ma_close_5 = Column(Float(precision=2, asdecimal=True))    # 日均线
    ma_close_10 = Column(Float(precision=2, asdecimal=True))   # 日均线
    ma_close_20 = Column(Float(precision=2, asdecimal=True))   # 日均线
    ma_close_30 = Column(Float(precision=2, asdecimal=True))   # 日均线
    ma_close_60 = Column(Float(precision=2, asdecimal=True))   # 日均线
    ma_close_120 = Column(Float(precision=2, asdecimal=True))  # 日均线
    ma_close_250 = Column(Float(precision=2, asdecimal=True))  # 日均线

    # vol
    ma_vol_3 = Column(Float(precision=2, asdecimal=True))    # 平均成交量
    ma_vol_5 = Column(Float(precision=2, asdecimal=True))
    ma_vol_10 = Column(Float(precision=2, asdecimal=True))
    ma_vol_20 = Column(Float(precision=2, asdecimal=True))
    ma_vol_30 = Column(Float(precision=2, asdecimal=True))
    ma_vol_60 = Column(Float(precision=2, asdecimal=True))
    ma_vol_120 = Column(Float(precision=2, asdecimal=True))
    ma_vol_250 = Column(Float(precision=2, asdecimal=True))

    # amount
    ma_amount_3 = Column(Float(precision=2, asdecimal=True))    # 平均成交额
    ma_amount_5 = Column(Float(precision=2, asdecimal=True))
    ma_amount_10 = Column(Float(precision=2, asdecimal=True))
    ma_amount_20 = Column(Float(precision=2, asdecimal=True))
    ma_amount_30 = Column(Float(precision=2, asdecimal=True))
    ma_amount_60 = Column(Float(precision=2, asdecimal=True))
    ma_amount_120 = Column(Float(precision=2, asdecimal=True))
    ma_amount_250 = Column(Float(precision=2, asdecimal=True))

    # trf 
    pre_trf   = Column(Float(precision=4, asdecimal=True))    # 昨日实际换手率
    ma_trf_3  = Column(Float(precision=4, asdecimal=True))
    ma_trf_5  = Column(Float(precision=4, asdecimal=True))
    ma_trf_10 = Column(Float(precision=4, asdecimal=True))

    # 量比 (in 5 days)
    vol_ratio = Column(Float(precision=4, asdecimal=True))
    # 量比 (in 20 days)
    vol_ratio_long = Column(Float(precision=4, asdecimal=True))

    # vol_type
    vol_type = Column(String(4))
    bar_type = Column(String(4))

    # upstops
    up_type = Column(String(4))
    conseq_up_num = Column(Integer)
    upstops_p2 = Column(Integer)    # 过去 3 天涨停次数
    upstops_p4 = Column(Integer)    # 过去 5 天涨停次数
    upstops_p9 = Column(Integer)    # 过去 10 天涨停次数
    upstops_p19 = Column(Integer)   # 过去 20 天涨停次数

    # list days
    list_days = Column(Integer)     # 上市天数

    def __init__(self, ts_code, trade_date):
        self.ts_code = ts_code
        self.trade_date = trade_date

    def __repr__(self):
        return '<Stock %r>' % self.ts_code
