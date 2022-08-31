from sqlalchemy import Column, Integer, String, Boolean, Date, Float, UniqueConstraint
from sqlbase import Base


class Dragon(Base):
    """
    TODO: USE AK
    name	str	Y	名称
    close	float	Y	收盘价
    pct_change	float	Y	涨跌幅
    turnover_rate	float	Y	换手率
    amount	float	Y	
    l_sell	float	Y	龙虎榜卖出额
    l_buy	float	Y	
    l_amount	float	Y	龙虎榜成交额
    net_amount	float	Y	龙虎榜净买入额
    net_rate	float	Y	
    amount_rate	float	Y	
    float_values	float	Y	
    reason	str	Y	
    """
    __tablename__ = 'dragon'

    id = Column(Integer, primary_key=True)
    ts_code = Column(String(12), index=True)
    trade_date = Column(Date, index=True)
    reason = Column(String(), index=True)                        #上榜理由
    name = Column(String(12), index=True)


    close = Column(Float(precision=2, asdecimal=True))           # 当日收盘
    pct_change = Column(Float(precision=4, asdecimal=True))      # 涨跌幅
    turnover_rate = Column(Float(precision=4, asdecimal=True))   # 换手率
    amount = Column(Float(precision=3, asdecimal=True))          # 总成交额
    l_sell = Column(Float(precision=4, asdecimal=True))          # 龙虎榜卖出额
    l_buy = Column(Float(precision=4, asdecimal=True))           # 龙虎榜买入额
    l_amount = Column(Float(precision=4, asdecimal=True))        # 龙虎榜成交额
    net_amount = Column(Float(precision=4, asdecimal=True))      # 龙虎榜净买入额
    net_rate = Column(Float(precision=4, asdecimal=True))        # 龙虎榜净买额占比
    amount_rate = Column(Float(precision=4, asdecimal=True))     # 龙虎榜成交额占比
    float_values = Column(Float(precision=4, asdecimal=True))    # 当日流通市值

    __table_args__ = (UniqueConstraint('ts_code', 'trade_date', 'reason', name='_dragon_uniq'),)


    def __init__(self, ts_code, trade_date):
        self.ts_code = ts_code
        self.trade_date = trade_date


    def __repr__(self):
       return '<Stock %r>' % self.ts_code
