from os.path import exists, dirname, abspath

import warnings
import functools
import pendulum as pdl
from functools import wraps
from datetime import timedelta, date, datetime
from pandas import Timestamp

from utils.type_helpers import *

import akshare as ak

trade_days_file = f'{dirname(abspath(__file__))}/../data/trade_days.txt'
if exists(trade_days_file):
    with open(trade_days_file, 'r') as f:
        all_trade_days = list(map(lambda t:t.strip(), f.readlines()))
else:
    all_trade_days = list(map(lambda t: t, ak.tool_trade_date_hist_sina()['trade_date']))
    with open(trade_days_file, 'w') as f:
        for t in all_trade_days:
            f.write(t + '\n')
    print(f'Trade date file {trade_days_file} is updated.')

all_trade_days = ['1990-12-10'] + all_trade_days
today_date = pdl.today().to_date_string()

def daterange(start_date, end_date):
    start_date = pdl.parse(start_date)
    end_date = pdl.parse(end_date)
    for n in range(int((end_date - start_date).days) + 1):
        yield (start_date + timedelta(n)).to_date_string()


def str_to_date(date_str):
    if isinstance(date_str, Timestamp):
        return datetime(
            date_str.year,
            date_str.month,
            date_str.day,
        ).date()
    elif isinstance(date_str, str):
        return datetime(
            pdl.parse(date_str).year,
            pdl.parse(date_str).month,
            pdl.parse(date_str).day,
        ).date()


def format_date(x, date_tickers):
    if x<0 or x>len(date_tickers)-1:
        return ''
    return date_tickers[int(x)]


class TradeDays():

    def __init__(self):
        self.__all_trade_days__ = all_trade_days


    def all_trade_days(self):
        return self.__all_trade_days__


    def trade_days_between(self, start_date, end_date, open_range=True):
        trade_days = self.all_trade_days()
        if open_range:
            return list(filter(lambda t: (t > start_date) and (t <= end_date), trade_days))
        else:
            return list(filter(lambda t: (t >= start_date) and (t <= end_date), trade_days))


    # all params are string
    def past_trade_days(self, end_date=None, days=0):
        if end_date is None:
            end_date = pdl.today().to_date_string()
        trade_days = self.all_trade_days()
        past_trade_days = list(filter(lambda t: t <= end_date, trade_days))
        if days > 0:
            return past_trade_days[-days:]
        else:
            return past_trade_days


    def non_trading_days(self, start_date, end_date):
        all_natural_days = daterange(start_date, end_date)
        return subtract(all_natural_days, self.trade_days_between(start_date, end_date, open_range=False))


    def future_trade_days(self, start_date=None):
        if start_date is None:
            start_date = pdl.today().to_date_string()
        trade_days = self.all_trade_days()
        return list(filter(lambda t: t > start_date, trade_days))



# DateTime Utils
trade_day_util = TradeDays()
today = pdl.now()
if today.to_date_string() == trade_day_util.past_trade_days()[-1] and today.hour < 18:
    end_date = trade_day_util.past_trade_days()[-2]
else:
    end_date = trade_day_util.past_trade_days()[-1]
start_date = trade_day_util.past_trade_days(end_date)[-255]
triquater_ago_date = trade_day_util.past_trade_days(end_date)[-200]
biquater_ago_date = trade_day_util.past_trade_days(end_date)[-140]
quater_ago_date = trade_day_util.past_trade_days(end_date)[-70]
month_ago_date = trade_day_util.past_trade_days(end_date)[-22]
bimonth_ago_date = trade_day_util.past_trade_days(end_date)[-45]
week_ago_date = trade_day_util.past_trade_days(end_date)[-7]
yesterday_date = trade_day_util.past_trade_days(end_date)[-2]
next_date = trade_day_util.future_trade_days(start_date=end_date)[0]
