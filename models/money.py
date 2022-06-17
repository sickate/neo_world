from sqlalchemy import Column, Integer, String, Boolean, Date, Float, UniqueConstraint
from sqlbase import Base

# vol 单位手
# amount 单位万
class Money(Base):

    __tablename__ = 'money'

    id = Column(Integer, primary_key=True)
    ts_code = Column(String(12), index=True)
    trade_date = Column(Date, index=True)
    buy_sm_vol = Column(Integer)
    buy_md_vol = Column(Integer)
    buy_lg_vol = Column(Integer)
    buy_elg_vol = Column(Integer)
    sell_sm_vol = Column(Integer)
    sell_md_vol = Column(Integer)
    sell_lg_vol = Column(Integer)
    sell_elg_vol = Column(Integer)

    buy_sm_amount = Column(Float(precision=3, asdecimal=True))
    buy_md_amount = Column(Float(precision=3, asdecimal=True))
    buy_lg_amount = Column(Float(precision=3, asdecimal=True))
    buy_elg_amount = Column(Float(precision=3, asdecimal=True))
    sell_sm_amount = Column(Float(precision=3, asdecimal=True))
    sell_md_amount = Column(Float(precision=3, asdecimal=True))
    sell_lg_amount = Column(Float(precision=3, asdecimal=True))
    sell_elg_amount = Column(Float(precision=3, asdecimal=True))

    net_mf_vol = Column(Integer)
    net_mf_amount = Column(Float(precision=3, asdecimal=True))

    __table_args__ = (UniqueConstraint('ts_code', 'trade_date', name='_money_uniq'),)


    def __init__(self, ts_code, trade_date):
        self.ts_code = ts_code
        self.trade_date = trade_date


















