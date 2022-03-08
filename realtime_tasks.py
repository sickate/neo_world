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


def monitor():
    dc, df = init_data(bimonth_ago_date, end_date, expire_days=30)
    finished, holding, orders = trade_summary2(month_ago_date, end_date, df)

    appendix = list(map(lambda x: get_ts_code_from_name(x), ['天宇股份', '浩通科技', '龙磁科技', '欣锐科技', '星帅尔', '科森科技', '沪电股份', '诚迈科技', '露笑科技', '万马股份', '蔚蓝锂芯', '文灿股份', '中天科技', '雷曼光电', '泉峰汽车', '宝通科技', '香山股份', '长信科技', '首都在线']))
    monitor_list = holding.reset_index()['ts_code'].to_list() + appendix

    # monitor 
    prev_top_stocks = None

    cons = ak_all_plates().set_index('ts_code').drop(columns='index')

    # all_big_deals = pd.read_feather(f'./tmp/{self.trade_date}_big_deal.feather')
    # for tmp in ak_loop_big_deal(all_big_deals):
    for tmp in ak_loop_big_deal():
        print('==================================================')

        print('持仓情况')
        price = ak_today_price(monitor_list)
        price = price[['current', '成交额', '成交量', 'pct_chg']]
        price.columns = ['current', 'total_amt', 'total_vol', 'pct_chg']

        print('-----------------------------------------------------')
        print(f'大单情况')
        if 'net_amt' in tmp.columns:
            sum_df = tmp.join(price)
            sum_df.loc[:, 'net_pct'] = cn_round_price(sum_df.net_amt / sum_df.total_amt * 100)
            display(sum_df.sort_values('net_pct', ascending=False).head(10))
        else:
            total = tmp
        sleep(5)

        print('市场情绪')
        print('')
        print('-----------------------------------------------------')
        ak_activity(False)
        print('-----------------------------------------------------')
        print('')
        sleep(5)

        con_sum, ind_sum, allstocks = ak_latest_prices(cons, verbose=True)

        topstocks = allstocks[allstocks.pct_chg>=7]
        if prev_top_stocks is None:
            prev_top_stocks = topstocks
            print('TOP Stocks:')
            print('------------------------------------------')
            print(topstocks)
        else:
            print('')
            print('Update TOP Stocks:')
            print('------------------------------------------')
            print('Dropped Stocks:')
            dropped_index = prev_top_stocks.loc[prev_top_stocks.index.difference(topstocks.index)].index
            display(allstocks.loc[dropped_index])

            print('')
            print('------------------------------------------')
            print('Upped Stocks:')
            display(topstocks.loc[topstocks.index.difference(prev_top_stocks.index)])
            sleep(5)
            prev_top_stocks = topstocks

