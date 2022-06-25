import sys
sys.path.append('./')

from env import os, glob, pdl, pd, msno, trange, tqdm, sleep, timeit, timedelta, ROOT_PATH
from models import *
from utils.stock_utils import *
from utils.stock_filter import *
from utils.psql_client import *
from utils.logger import logger


class DataCenter:
    def __init__(self, start_date, end_date, trade_days=None, stock_list=None):
        self.start_date = start_date
        self.end_date = end_date
        self.trade_days = tdu.past_trade_days(end_date) if trade_days is None else trade_days
        if stock_list:
            stocks = get_stock_basic(end_date)
            self.stock_list = stocks[stocks.index.isin(stock_list)]
        else:
            self.stock_list = None
        self.stock_basics = None
        self.price = None
        self.money_flow = None
        self.upstops = None
        self.auctions = None
        self.dfall = None


    def get_money_flow(self, force_refresh=False):
        if self.money_flow is None or force_refresh:
            money_ts = load_table(Money, self.start_date, self.end_date)
            self.money_flow = money_ts.sort_index()
        return self.money_flow


    def get_price(self, force_refresh=False, calc_adj=True, additional_vars=True):
        if self.price is None or force_refresh:
            self.price = load_stock_prices(start_date=self.start_date, end_date=self.end_date, fast_load=True)
            if calc_adj:
                self.price = gen_adj_price(self.price, replace=True)
            if self.stock_list is not None:
                self.price = StockFilter(self.end_date, self.stock_list).hs().filter(self.price)
            self.price = gen_price_data(self.price)

            # TODO: finalized it
            # calc more attributes
            # open_pct: 开盘集合竞价涨幅
            self.price.loc[:, 'open_pct'] = (self.price.open - self.price.pre_close)/self.price.pre_close * 100
            self.price.loc[:, 'next_open_pct'] = self.price.groupby(level='ts_code').open_pct.shift(-1)
            self.price = calc_vol_types(self.price, mavgs=[5,20])

            self.price.sort_index(inplace=True)
        return self.price


    def get_stock_basics(self, force_refresh=False):
        if self.stock_basics is None or force_refresh:
            self.stock_basics = read_pg(table=StockBasic.__tablename__).set_index('ts_code')
            self.stock_basics.loc[:,'list_date'] = self.stock_basics.list_date.map(str).apply(lambda x: x[0:4]+'-'+x[4:6]+'-'+x[-2:])
            if self.stock_list is not None:
                self.stock_basics = StockFilter(self.end_date, self.stock_list).hs().filter(self.stock_basics)
                # self.stock_basics = filter_bad(self.stock_basics, self.stock_list)
        return self.stock_basics


    def get_upstops(self, force_refresh=False, slim=False):
        if self.upstops is None or force_refresh:
            upstops = load_table(UpStop, self.start_date, self.end_date)
            self.upstops = upstops.sort_index().drop(columns=['name'])
        if slim:
            return self.upstops[['amp','fc_ratio','fl_ratio','fd_amount','first_time', 'last_time', 'open_times', 'strth', 'limit']]
        else:
            return self.upstops


    def init_all(self, force_refresh=False):
        self.get_stock_basics(force_refresh)
        self.get_money_flow(force_refresh)
        self.get_price(force_refresh)
        self.get_upstops(force_refresh)


    def merge_all(self):
        print('Merging all data...')
        stk_basic = self.get_stock_basics()
        price = self.get_price()
        upstop = self.get_upstops()
        mf = self.get_money_flow()
        df_init = price.join(mf).join(upstop.drop(columns=['pct_chg', 'close'])).join(stk_basic[['name', 'list_date']])
        df_init = df_init[~df_init.list_date.isna()] # remove already 退市的

        # 计算复合指标
        # 涨停类型
        print('Calculating upstop data...')
        df_init = calc_upstop(df_init)
        df_init.loc[:, 'next_limit']   = df_init.groupby(level='ts_code').limit.shift(-1)
        df_init.loc[:, 'next_up_type'] = df_init.groupby(level='ts_code').up_type.shift(-1)
        # 涨停复合计算
        # 均线计算
        print('Calculating MAs...')
        df_init = gen_ma(df_init, mavgs=[5, 10, 30, 60, 120, 250])
        df_init.loc[:,'pre_ma_close_5'] = df_init.groupby(level='ts_code').ma_close_5.shift(1)
        df_init.loc[:,'pre2_ma_close_5'] = df_init.groupby(level='ts_code').ma_close_5.shift(2)
        df_init.loc[:,'pre3_ma_close_5'] = df_init.groupby(level='ts_code').ma_close_5.shift(3)

        # 计算 list_days
        print('Calculating list_days...')
        # df_init['list_date'] = df_init.list_date.apply(lambda x: x[0:4]+'-'+x[4:6]+'-'+x[-2:])
        df_init['cur_date'] = df_init.index.get_level_values('trade_date').map(lambda x: x.strftime('%Y-%m-%d'))
        tmp = df_init.loc[df_init.groupby('ts_code').head(1).index]
        tmp['list_days'] = tmp.cur_date.map(lambda x: self.trade_days.index(x)) - \
                           tmp.list_date.map(lambda x: self.trade_days.index(x)) + 1
        df_init['list_days'] = tmp['list_days']
        df_init.list_days.fillna(1, inplace=True)
        df_init.list_days = df_init.groupby('ts_code').list_days.cumsum()

        # 计算 bar type
        df_init.loc[:, 'bar_type'] = df_init.apply(f_calc_yinyang, axis=1)
        df_init.loc[:, 'pre_bar_type'] = df_init.groupby('ts_code').bar_type.shift(1)
        df_init.loc[:, 'pre2_bar_type'] = df_init.groupby('ts_code').bar_type.shift(2)

        auctions = load_table(Auction, self.start_date, self.end_date)
        self.auctions = auctions.sort_index()
        df_init = df_init.join(self.auctions[['auc_vol', 'auc_amt']])

        print('Performing shift to get prev signals...')
        df_init.loc[:, 'cvo'] = df_init.pct_chg - df_init.open_pct
        for ind in ['open_times', 'fl_ratio', 'fc_ratio', 'strth', 'amount', 'amp', 'vol', 'vol_ratio']:
            df_init.loc[:, f'pre_{ind}'] = df_init.groupby(level='ts_code')[ind].shift(1)
        for ind in ['cvo', 'auc_amt', 'auc_vol', 'bar_type', 'vol_ratio']:
            df_init.loc[:, f'next_{ind}'] = df_init.groupby(level='ts_code')[ind].shift(-1)
        df_init.loc[:, 'next_auc_pvol_ratio'] = df_init.next_auc_amt/df_init.amount

        df_init.loc[:, 'dde_amt'] = (df_init.buy_elg_amount + df_init.buy_lg_amount - df_init.sell_elg_amount - df_init.sell_lg_amount) * 10 # unit从万变成千
        df_init.loc[:, 'dde_vol'] = (df_init.buy_elg_vol + df_init.buy_lg_vol - df_init.sell_elg_vol - df_init.sell_lg_vol) / 10 # unit从手换成千股
        df_init.loc[:, 'dde'] = round(df_init.dde_vol / df_init.float_share * 10, 2) # 千股除以万股，/10,再换成 pct，*100 =》 *10

        self.dfall = df_init
        return self.dfall


def init_data(start_date, end_date, expire_days=30):
    print(f'Initializing data from {start_date} to {end_date}...')
    dc = DataCenter(start_date, end_date)

    search_pattern = glob.glob(f'{ROOT_PATH}/tmp/price_{start_date}_{end_date}_*.feather')
    for f in search_pattern:
        # read cache
        print(f'Found cache file: {f}, loading...')
        df_init = pd.read_feather(f).set_index(['ts_code', 'trade_date'])
        break
    else:
        df_init = dc.merge_all()
        # cache it
        expire_date = pdl.today().add(days=expire_days).to_date_string()
        df_file_path = f'{ROOT_PATH}/tmp/price_{start_date}_{end_date}_{expire_date}.feather'
        df_init.reset_index().to_feather(df_file_path)
    return dc, df_init


if __name__ == '__main__':
    dc = DataCenter(start_date, end_date=today_date)
    price = dc.get_price()
    print(price)
