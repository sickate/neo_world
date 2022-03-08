from sqlalchemy import Column, Integer, String, Boolean, Date, Float, UniqueConstraint
from sqlbase import Base


class AdjFactor(Base):
    """
    Basic info of stock every day

    * 接口：adj_factor
    * 更新时间：早上9点30分
    * 描述：获取股票复权因子，可提取单只股票全部历史复权因子，也可以提取单日全部股票的复权因子。
    """

    __tablename__ = 'adj_factor'

    id = Column(Integer, primary_key=True)
    ts_code = Column(String(12), index=True)
    trade_date = Column(Date, index=True)
    adj_factor = Column(Float(precision=2, asdecimal=True))

    __table_args__ = (UniqueConstraint('ts_code', 'trade_date', name='_adj_factor_uniq'),)

    def __init__(self, ts_code, trade_date, adj_factor):
        self.ts_code = ts_code
        self.trade_date = trade_date
        self.adj_factor = adj_factor
