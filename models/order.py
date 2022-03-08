import sys
sys.path.append('./')
from env import pdl
from utils.calculators import cn_round_price

from enum import Enum
from sqlalchemy import Column, Integer, String, Boolean, Date, DateTime, Float, UniqueConstraint
from sqlalchemy.dialects import postgresql
from sqlbase import Base

class Order(Base):

    __tablename__ = 'orders'
    __table_args__ = (UniqueConstraint('ts_code', 'order_at', name='_order_uniq'),)

    id = Column(Integer, primary_key=True)

    ts_code = Column(String(12), index=True)
    trade_date = Column(Date, index=True)

    vol = Column(Integer) # Buy/Sell: Pos/Neg int
    price = Column(Float(precision=3, asdecimal=True)) # compare with high, low, open, close
    order_at = Column(DateTime)
    fee = Column(Float(precision=2, asdecimal=True))
    tax = Column(Float(precision=2, asdecimal=True))

    reason = Column(String)
    trader = Column(String)


    def __init__(self, ts_code, order_at, trader='Tuo'):
        self.ts_code = ts_code
        self.order_at = order_at
        self.trade_date = pdl.parse(order_at).to_date_string()
        self.trader = trader


    @property
    def amount(self):
        return abs(self.vol * self.price)


    def write_log(self, vol, price):
        self.vol = vol
        self.price = price
        amount = cn_round_price(abs(price * vol))
        self.tax = cn_round_price(0 if vol > 0 else amount * 0.001)
        self.fee = cn_round_price(amount * 0.0002 if amount * 0.0002 > 5 else 5)


    def __str__(self):
        action_str = '买入' if self.vol > 0 else '卖出'
        return f'[{self.order_at}] {action_str} {self.ts_code} {abs(self.vol)} 股, 均价 {float(self.price)} 元, 总金额 {float(abs(self.vol * self.price))} 元, 税费 {float(self.tax)}、{float(self.fee)} 元.'

