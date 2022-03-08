from enum import Enum
from sqlalchemy import Column, Integer, String, Boolean, Date, Float, UniqueConstraint
from sqlalchemy.dialects import postgresql
from sqlbase import Base

class PriceMovement(Enum):
    UP = 'up'
    MUP = 'mini_up'
    DN = 'down'
    MDN = 'mini_down'
    FL = 'flat'
    QU = 'question'


class Memo(Base):
    """
    """
    PRED_DIR = PriceMovement

    __tablename__ = 'memos'

    id = Column(Integer, primary_key=True)

    ts_code = Column(String(12), index=True)

    # watch
    watch_date =  Column(Date, index=True)

    # open
    open_date = Column(Date, index=True)
    open_price = Column(Float(precision=3, asdecimal=True)) # compare with high, low, open, close
    share = Column(Integer)
    reason = Column(String)

    # close
    close_date = Column(Date, index=True)
    close_price = Column(Float(precision=3, asdecimal=True)) # compare with high, low, open, close
    reason = Column(String)

    dtype = Column(String) # comment type: BASIC, TECH,
    # price_by_time = Column(postgresql.ARRAY(Float(precision=2, asdecimal=True), dimensions=1))   # 涨跌幅
    pred_dir = Column(String) # predicted direction : UP, MUP, DN, MDN, FL(AT), QU(ESTION)
    comment = Column(String)

    pred_chg_1 = Column(Float(precision=2, asdecimal=True)) # reserved next day pct_chg
    pct_chg_1 = Column(Float(precision=2, asdecimal=True))

    __table_args__ = (UniqueConstraint('ts_code', 'watch_date', 'dtype', name='_memos_uniq'),)


    def __init__(self, ts_code, watch_date, dtype='BASIC'):
        self.ts_code = ts_code
        self.watch_date = watch_date
        self.dtype = dtype

