"""
对 ak 和 ts 的封装，取数据
"""

from os.path import exists, dirname, abspath
import sys
sys.path.append('./')

from env import np, pd, pdl, trange, tqdm, display, sleep
from sqlbase import engine

from utils.psql_client import insert_df
from utils.stock_utils import *

AK_TICK_COLUMNS = ['time', 'price', 'change', 'volume', 'amount', 'direction']
MAJOR_INDICES = ['上证指数', '深证成指', '创业板指', '科创50', '中小综指', '创业板综', '上证50', '沪深300', '中证500', '中证1000']

import akshare as ak
import tushare as ts

ts.set_token('5f576fde2efd1ac4df59161bda0a4f04ac599535db9a1ffec1de21de')
pro = ts.pro_api()


#####################################################################
# Tushare
#####################################################################

def ts_today_price():
    """
        Tushare 获取当天实时价格, Slow
    """
    tdf = ts.get_today_all()
    td = tdf.rename(columns={'code': 'ts_code', 'changepercent':'pct_chg', 'trade':'close', 'volume':'vol'})
    td.loc[:,'ts_code'] = td.ts_code.map(add_postfix)
    td.loc[:,'trade_date'] = pdl.today().date()
    td = td.set_index(['ts_code', 'trade_date'])[['open','high','low','close', 'vol', 'amount','pct_chg']]
    return td


#####################################################################
# AkShare
#####################################################################

def ak_all_plates(use_cache=True):
    '''
        Get all plate stocks
    '''
    cons_file = f'{dirname(abspath(__file__))}/../data/plates.feather'
    if use_cache:
        return pd.read_feather(cons_file)
    else:
        con_df = pd.DataFrame()
        cons = ak.stock_board_concept_name_ths()
        cons.columns=['trade_date','name', 'stk_count', 'url']
        inds = ak.stock_board_industry_name_ths()
        sleep_counter = 0
        for name in cons.name:
            tmp = ak.stock_board_concept_cons_ths(symbol=name)
            tmp['plate_type'] = 'concept'
            con_df, sleep_counter = process_plate_res(con_df, tmp, name, sleep_counter)
        for name in inds.name:
            tmp = ak.stock_board_industry_cons_ths(symbol=name)
            tmp['plate_type'] = 'industry'
            con_df, sleep_counter = process_plate_res(con_df, tmp, name, sleep_counter)
        con_df.reset_index().to_feather(cons_file)
        return con_df


def ak_today_price(monitor_list=None):
    summary = """
        AkShare 获取当天实时价格
    """
    df = ak.stock_zh_a_spot_em()
    df.rename(columns={'代码':'ts_code', '最新价':'current', '涨跌幅': 'pct_chg', '换手率': 'turnover_rate', '量比': 'vol_ratio' , '名称':'name'}, inplace=True)
    df['ts_code'] = df.ts_code.apply(add_postfix)
    if monitor_list:
        display(df[df.ts_code.isin(monitor_list)])
    return df.set_index('ts_code')


def ak_today_auctions(stk_basic, save_db=True):
    res = pd.DataFrame()
    for ts_code in tqdm(stk_basic.index):
        code = ts_code.split('.')[0]
        mkt = ts_code.split('.')[1]
        if mkt == 'BJ': # skip 北交所
            continue
        stock_zh_a_hist_pre_min_em_df = ak.stock_zh_a_hist_pre_min_em(symbol=code)
        stock_zh_a_hist_pre_min_em_df.columns = ['time', 'open', 'close', 'high', 'low', 'auc_vol', 'auc_amt', 'latest']
        stock_zh_a_hist_pre_min_em_df.loc[:,'ts_code'] = ts_code
        res = res.append(stock_zh_a_hist_pre_min_em_df.loc[11])

    res.loc[:,'trade_date'] = res['time'].apply(lambda t: t.split(' ')[0])
    res.set_index(['ts_code', 'trade_date'], inplace=True)

    if save_db:
        today = res.reset_index()['trade_date'].at[0]
        yesterday = tdu.past_trade_days(end_date=today)[-2]
        pre_price = load_table(model=Price, start_date=yesterday, end_date=yesterday).droplevel('trade_date').rename({'close':'pre_close'})
        auctions = res.join(pre_price[['pre_close']])
        auctions.loc[:,'open_pct']=round((auctions.open/auctions.pre_close-1) * 100, 2)
        auctions = auctions[['open', 'pre_close', 'open_pct', 'auc_vol', 'auc_amt']].reset_index()
        insert_df(df=auctions, tablename='auction')

    return res


def ak_today_index():
    stock_zh_index_spot_df = ak.stock_zh_index_spot()
    stock_zh_index_spot_df.columns = ['code', 'index_name', 'latest', 'change', 'pct_chg', 'pre_close', 'open', 'high', 'low', 'vol', 'amount']
    major_df = stock_zh_index_spot_df[stock_zh_index_spot_df.index_name.isin(MAJOR_INDICES)]
    return stock_zh_index_spot_df, major_df


def ak_activity(save=False, verbose=True):
    summary = """
        AkShare 获取当天实时活跃度
    """
    stock_legu_market_activity_df = ak.stock_market_activity_legu().set_index('item').transpose()
    if verbose:
        display(stock_legu_market_activity_df)
    cols = ['up', 'upstop','real_upstop', 'st_upstop', 'dn', 'dnstop', 'real_dnstop', 'st_dnstop', 'fl', 'halt', 'vitality', 'trade_date']
    stock_legu_market_activity_df.columns = cols
    stock_legu_market_activity_df.trade_date = stock_legu_market_activity_df.trade_date.apply(lambda x: x.split(' ')[0])
    stock_legu_market_activity_df.vitality = stock_legu_market_activity_df.vitality.apply(lambda x: float(x.strip('%')))
    for col in cols[0:-2]:
        stock_legu_market_activity_df[col] = stock_legu_market_activity_df[col].apply(lambda x: int(x))
    if save:
        insert_df(stock_legu_market_activity_df, 'activities')
    return stock_legu_market_activity_df.set_index('trade_date')


########################################
# Complex Monitoring Methods
########################################

def ak_loop_big_deal(total=None):
    summary = '''
        Read big deals everytime gets called.
    '''
    if not total:
        total = pd.DataFrame()
    while True:
        df1 = ak.stock_fund_flow_big_deal()
        df1.columns = ['time', 'ts_code', 'name', 'price', 'vol', 'amount', 'direction', 'pct_chg', 'change']
        df1.loc[:,'ts_code'] = df1.ts_code.map(lambda x: add_postfix('%06d' % x))
        df1.loc[:,'amount'] = df1.amount * 10000
        total = pd.concat([total, df1]).drop_duplicates()
        total.reset_index().to_feather(f'{dirname(abspath(__file__))}/../tmp/today_big_deal.feather')
        cur_sum = total.groupby(['ts_code', 'direction']).agg({'amount':'sum', 'vol': 'sum'}).pivot_table(index='ts_code', columns=['direction'])
        cur_sum.columns = ['_'.join(col) for col in cur_sum.columns]
        print(cur_sum.columns)
        cur_sum = cur_sum.fillna(value=0)
        cur_sum.loc[:, 'net_amt'] = cur_sum['amount_买盘'] - cur_sum['amount_卖盘']
        cur_sum.loc[:, 'net_vol'] = cur_sum['vol_买盘'] - cur_sum['vol_卖盘']
        cur_sum.columns = ['buy_amt', 'sell_amt', 'buy_vol', 'sell_vol', 'net_amt', 'net_vol']
        if pdl.now().hour <= 15:
            yield cur_sum
        else:
            yield total
            break


def ak_latest_prices(cons, verbose=True, display_latency=5):
    price = ak_today_price().rename(columns={'成交量':'amount', '成交额':'vol', '昨收':'pre_close'})
    price = price.join(cons)

    # pre calc
    price.loc[:,'up7'] = price.pct_chg.map(lambda x: 1 if x>=7 else 0)
    price.loc[:,'up10'] = price.pct_chg.map(lambda x: 1 if x>=9.92 else 0)
    price.loc[:,'dn7'] = price.pct_chg.map(lambda x: 1 if x<=-7 else 0)
    price.loc[:,'dn10'] = price.pct_chg.map(lambda x: 1 if x<=-9.92 else 0)

    # calc plate
    plate_sum = price.reset_index().groupby(['plate_type', 'plate_name']).agg({
        'pct_chg':'mean', 'amount':'sum', 'ts_code':'count',
        'up7':'sum', 'up10':'sum', 'dn7':'sum', 'dn10': 'sum'
    })

    con_sum = plate_sum.xs('concept', level='plate_type', drop_level=True)
    if verbose:
        print('')
        print('Top 概念：')
        print('------------------------------------------')
        display(con_sum.sort_values('pct_chg', ascending=False).head(10))
        sleep(display_latency)

    ind_sum = plate_sum.xs('industry', level='plate_type', drop_level=True)
    if verbose:
        print('')
        print('Top 行业：')
        print('------------------------------------------')
        display(ind_sum.sort_values('pct_chg', ascending=False).head(10))
        sleep(display_latency)

    # 筛选涨幅大的个股
    upstocks = price
    # 保留涨幅大的概念
    upstocks = upstocks.reset_index().set_index('plate_name').join(con_sum.rename(columns={'pct_chg':'concept_pct'})[['concept_pct']])
    upstocks = upstocks.reset_index().groupby('ts_code').apply(lambda x: x.nlargest(3,['concept_pct'])).reset_index(drop=True)
    # 合并概念到个股
    upstocks['concepts'] = upstocks.groupby('ts_code')['plate_name'].transform(lambda x: ', '.join(x))
    topstocks = upstocks[['ts_code', 'name', 'pct_chg', 'concepts']].drop_duplicates().set_index('ts_code').sort_values('pct_chg', ascending=False)
    # 合并行业到个股
    topstocks = topstocks.join(cons[cons.plate_type=='industry'][['plate_name']].rename(columns={'plate_name':'industry'}))

    return con_sum, ind_sum, topstocks


########################################
# Helpers
########################################

def process_plate_res(con_df, tmp, name, sleep_counter):
    tmp1 = tmp.rename(columns={'代码':'ts_code'})[['ts_code', 'plate_type']]
    tmp1.ts_code = tmp1.ts_code.apply(add_postfix)
    tmp1['plate_name'] = name
    if len(con_df) == 0:
        con_df = tmp1
    else:
        con_df = con_df.append(tmp1)
    sleep_counter += 1
    if sleep_counter % 3 == 1:
        sleep(10)
    return con_df, sleep_counter


if __name__ == '__main__':
    tp = fetch_today_price()
    print(tp)


