import sys
sys.path.append('./')

from env import *

from sqlbase import Base, engine, meta, db_session
from models import *

from utils.datetimes import *
from utils.argparser import data_params_wrapper
from utils.psql_client import load_table, insert_df
from data_center import *
from utils.datasource import *
from utils.stock_utils import *
from utils.pd_styler import *

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 120)
pd.set_option('display.width', 200)
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)


class DataMonitor():

    def __init__(self, trade_date=None):
        self.trade_date = pdl.today().to_date_string()
        self.dc, self.df = init_data(month_ago_date, trade_date, expire_days=30)
        self.cons = ak_all_plates().set_index('ts_code').drop(columns='index')

        # order list
        self.finished, self.holding, self.orders = trade_summary2(month_ago_date, trade_date, dc)

        # today top stocks
        self.prev_top_stocks = None

        # today big deals
        self.big_deal_df = pd.DataFrame()

        appendix = list(map(lambda x: get_ts_code_from_name(x), ['三角防务', '长信科技', '沧州明珠','共达电声','露笑科技','国机汽车','英杰电气','信濠光电','中青宝','宝通科技']))
        self.monitor_list =  self.holding.reset_index()['ts_code'].to_list() + appendix


    def checkpoint(self, cache_to_file=True):
        if cache_to_file:
            self.big_deal_df.reset_index().to_feather(f'./tmp/{self.trade_date}_big_deal.feather')


    def recover(self, cache_to_file=True):
        pass


    def update_prices(self):
        con_sum, ind_sum, allstocks = ak_latest_prices(cons, verbose=True)
        topstocks = allstocks[allstocks.pct_chg>=7]

        # show con, ind sums
        # show

    def update_big_deals(self):
        # append self.big_deal_df
        # join price
        sum_df = tmp.join(price[['total_amt', 'total_vol', 'pct_chg']])
        sum_df.loc[:, 'net_pct'] = cn_round_price(sum_df.net_amt / sum_df.total_amt * 100)
        display(sum_df.sort_values('net_pct', ascending=False).head(10))
        pass


    def update_activity(self):
        pass


    def update_auctions(self):
        pass



