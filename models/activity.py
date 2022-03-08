from sqlalchemy import Column, Integer, String, Boolean, Date, Float, UniqueConstraint
from sqlbase import Base


class Activity(Base):
    """
    * 接口：ak.stock_market_activity_legu()
    * 更新时间：Realtime
    * 描述：单次返回当前赚钱效应分析数据
    * # 涨跌比：即沪深两市上涨个股所占比例，体现的是市场整体涨跌，占比越大则代表大部分个股表现活跃。
      # 涨停板数与跌停板数的意义：涨停家数在一定程度上反映了市场的投机氛围。当涨停家数越多，则市场的多头氛围越强。真实涨停是非一字无量涨停。真实跌停是非一字无量跌停。
    """

    __tablename__ = 'activities'

    id = Column(Integer, primary_key=True)
    trade_date = Column(Date, index=True, unique=True)

    up = Column(Integer)
    upstop = Column(Integer)
    real_upstop = Column(Integer)
    st_upstop = Column(Integer)
    dn = Column(Integer)
    dnstop = Column(Integer)
    real_dnstop = Column(Integer)
    st_dnstop = Column(Integer)
    fl = Column(Integer)
    halt = Column(Integer)
    vitality = Column(Float)


    def __init__(self, trade_date):
        self.trade_date = trade_date
 
