from sqlalchemy import Column, Integer, String, Boolean, Date, Float, UniqueConstraint
from sqlbase import Base

class StockBasic(Base):
    """
    接口：Tushare stock_basic
    描述：获取基础信息数据，包括股票代码、名称、上市日期、退市日期等. 每天能取到截止前一天的
    """

    __tablename__ = 'stock_basic'

    id = Column(Integer, primary_key=True)
    ts_code = Column(String(12), index=True) # TS 代码
    symbol = Column(String(12), index=True) # 股票代码
    name = Column(String(12), index=True) # 股票名称
    area = Column(String(12)) # 所在地域
    industry = Column(String(12)) #所属行业
    fullname = Column(String(30)) #股票全称
    enname   = Column(String(100)) #英文全称
    market   = Column(String(12)) #市场类型 （主板/中小板/创业板/科创板）
    exchange = Column(String(12)) #交易所代码
    curr_type = Column(String(12)) # 交易货币
    list_status = Column(String(1))  # 上市状态： L上市 D退市 P暂停上市
    list_date = Column(String(12))  # 上市日期
    delist_date = Column(String(12)) # 退市日期
    is_hs = Column(String(3)) # 沪深港通标的，N否 H沪股通 S深股通


    __table_args__ = (UniqueConstraint('ts_code', name='_stock_basic_uniq'),)
