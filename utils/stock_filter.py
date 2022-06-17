import sys
sys.path.append('./')

from env import pdl
import random
from utils.psql_client import get_stock_basic

class StockFilter():
    def __init__(self, end_date, stocks=None):
        if isinstance(end_date, str):
            self.end_date = pdl.parse(end_date)
        else:
            self.end_date = end_date

        if stocks is None:
            self.stocks = get_stock_basic(end_date)
            self.stocks.loc[:,'list_date'] = self.stocks.list_date.map(lambda x: pdl.parse(x))
            # remove BJ by default
            self.stocks = self.stocks[~self.stocks.index.str.contains('BJ')]
        else:
            self.stocks = stocks

    def clone(sf):
        return StockFilter(sf.end_date, sf.stocks)

    def isin(self, lst):
        tmp_sf = StockFilter.clone(self)
        tmp_sf.stocks = tmp_sf.stocks[tmp_sf.stocks.index.isin(lst)]
        return tmp_sf

    def new(self):
        tmp_sf = StockFilter.clone(self)
        tmp_sf.stocks = tmp_sf.stocks[tmp_sf.stocks.list_date > tmp_sf.end_date.add(days=-60)]
        return tmp_sf

    def not_new(self):
        tmp_sf = StockFilter.clone(self)
        tmp_sf.stocks = tmp_sf.stocks[tmp_sf.stocks.list_date <= tmp_sf.end_date.add(days=-60)]
        return tmp_sf


    def st(self, anti=False):
        tmp_sf = StockFilter.clone(self)
        tmp_sf.stocks = tmp_sf.stocks[tmp_sf.stocks.name.str.contains('ST')]
        return tmp_sf

    def not_st(self):
        tmp_sf = StockFilter.clone(self)
        tmp_sf.stocks = tmp_sf.stocks[~tmp_sf.stocks.name.str.contains('ST')]
        return tmp_sf

    # 目前用不上，已经不包含退市
    def tui(self, anti=False):
        tmp_sf = StockFilter.clone(self)
        if anti:
            tmp_sf.stocks = tmp_sf.stocks[~tmp_sf.stocks.name.str.contains('退')]
        else:
            tmp_sf.stocks = tmp_sf.stocks[tmp_sf.stocks.name.str.contains('退')]
        return tmp_sf

    def hs(self):
        tmp_sf = StockFilter.clone(self)
        tmp_sf.stocks = tmp_sf.stocks[(tmp_sf.stocks.index.str.endswith('SZ')) | (tmp_sf.stocks.index.str.endswith('SH'))]
        return tmp_sf

    def cyb(self):
        tmp_sf = StockFilter.clone(self)
        tmp_sf.stocks = tmp_sf.stocks[tmp_sf.stocks.symbol.str.startswith('30')]
        return tmp_sf

    def kcb(self, anti=False):
        tmp_sf = StockFilter.clone(self)
        if anti:
            tmp_sf.stocks = tmp_sf.stocks[~tmp_sf.stocks.symbol.str.startswith('688')]
        else:
            tmp_sf.stocks = tmp_sf.stocks[tmp_sf.stocks.symbol.str.startswith('688')]

        return tmp_sf

    def zb(self):
        tmp_sf = StockFilter.clone(self)
        tmp_sf.stocks = tmp_sf.stocks[tmp_sf.stocks.symbol.str.startswith('00') | tmp_sf.stocks.symbol.str.startswith('60')]
        return tmp_sf

    def get_list(self, col='ts_code'):
        return self.stocks.reset_index().loc[:, col].to_list()

    def filter(self, df, limit=None):
        lst = self.get_list()
        if limit:
            random.shuffle(lst)
            lst = lst[0:limit]
        return df[df.index.isin(lst, level='ts_code')]


if __name__ == '__main__':
    from utils.datetimes import end_date
    print(f'Start fetching stock list for {end_date}')
    sf = StockFilter(end_date).not_new().not_st().cyb()
    print(sf.get_list(col='name')[0:5])
