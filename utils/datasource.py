"""
对 ak 和 ts 的封装，取数据
"""

import sys
import requests as r
from os import stat
from os.path import exists, dirname, abspath
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

def ak_all_plates(use_cache=True, major_update=False, verbose=False):
    '''
        Get all plate stocks
    '''
    cons_file = f'{dirname(abspath(__file__))}/../data/plates.feather'
    cached = pd.read_feather(cons_file)
    if len(cached) == 0:
        major_update = True
    print(f'Previous version has {len(cached)} records.')
    if use_cache:
        return cached
    else:
        updated_on = pdl.from_timestamp(stat(cons_file).st_mtime).date()
        if updated_on >= pdl.today().date():
            return pd.read_feather(cons_file)
        else:
            con_dfs = []
            cons = ak.stock_board_concept_name_ths()
            # 日期	概念名称	成分股数量	网址	代码
            # cons.columns=['trade_date', 'name', 'stk_count', 'url', 'symbol']
            inds = ak.stock_board_industry_name_ths()
            sleep_counter = 0
            print(f'Loading {len(cons)} concepts...')
            for name in tqdm(cons['概念名称']):
                if not major_update and len(cached[cached.plate_name==name])>0:
                    con_dfs.append(cached[cached['plate_name'] == name])
                    continue
                else:
                    print(f'Fetching new concept {name} ...')
                    tmp = ak.stock_board_concept_cons_ths(symbol=name)
                    tmp['plate_type'] = 'concept'
                    tmp_con_df, sleep_counter = process_plate_res(tmp, name, sleep_counter)
                    con_dfs.append(tmp_con_df)
            print(f'Loading {len(inds)} industries...')
            for name in tqdm(inds.name):
                if not major_update and len(cached[cached.plate_name==name])>0:
                    con_dfs.append(cached[cached['plate_name'] == name])
                    continue
                else:
                    print(f'Fetching new industry {name} ...')
                    tmp = ak.stock_board_industry_cons_ths(symbol=name)
                    tmp['plate_type'] = 'industry'
                    tmp_con_df, sleep_counter = process_plate_res(tmp, name, sleep_counter)
                    con_dfs.append(tmp_con_df)
            con_df = pd.concat(con_dfs)
            con_df = con_df[~con_df.duplicated(subset=['ts_code', 'plate_type', 'plate_name'])]
            con_df = con_df.reset_index().drop(columns=['level_0'])
            con_df.to_feather(cons_file)
            print(f'{len(con_df)} plates are loaded.')
            return con_df


def ak_tfp(end_date):
    summary = """
        AkShare 获取当天停复牌信息
    """
    end_date_ak = pdl.parse(end_date).strftime('%Y%m%d')
    stock_em_tfp_df = ak.stock_tfp_em(date=end_date_ak)
    stock_em_tfp_df.set_index('代码', inplace=True)
    stock_em_tfp_df = stock_em_tfp_df[~stock_em_tfp_df.index.str.startswith('8')]
    if len(stock_em_tfp_df[stock_em_tfp_df['停牌原因']=='交易异常波动'])>0:
        display(stock_em_tfp_df[stock_em_tfp_df['停牌原因']=='交易异常波动'])
    print('=================================================================================')
    display(stock_em_tfp_df[stock_em_tfp_df['停牌原因']!='交易异常波动'])
    return stock_em_tfp_df


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


def ak_today_auctions(ts_codes, save_db=True, open_mkt=True):
    res = pd.DataFrame()
    if open_mkt:
        ind = 11
    else:
        ind = 252
        save_db = False
    for ts_code in tqdm(ts_codes):
        code = ts_code.split('.')[0]
        mkt = ts_code.split('.')[1]
        if mkt == 'BJ': # skip 北交所
            continue
        try:
            stock_zh_a_hist_pre_min_em_df = ak.stock_zh_a_hist_pre_min_em(symbol=code)
            stock_zh_a_hist_pre_min_em_df.columns = ['time', 'open', 'close', 'high', 'low', 'auc_vol', 'auc_amt', 'latest']
            stock_zh_a_hist_pre_min_em_df.loc[:,'ts_code'] = ts_code
            res = res.append(stock_zh_a_hist_pre_min_em_df.loc[ind])
        except Exception as e:
            print(e)
            print(f'Trying get {code} got error.')

    res.loc[:,'trade_date'] = res['time'].apply(lambda t: t.split(' ')[0])
    res.loc[:, 'auc_amt'] = res.auc_amt/1000 # 金额单位改为k
    res.loc[:, 'auc_vol'] = res.auc_vol/10   # 手改为千股
    res.set_index(['ts_code', 'trade_date'], inplace=True)

    if save_db:
        today = res.reset_index()['trade_date'].at[0]
        yesterday = tdu.past_trade_days(end_date=today)[-2]
        pre_price = load_table(model=Price, start_date=yesterday, end_date=yesterday)[['close']].droplevel('trade_date').rename(columns={'close':'pre_close'})
        auctions = res.join(pre_price)
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
    stock_legu_market_activity_df.vitality = stock_legu_market_activity_df.vitality.apply(lambda x: float(x.split('%')[0]))
    for col in cols[0:-2]:
        stock_legu_market_activity_df[col] = stock_legu_market_activity_df[col].apply(lambda x: int(x))
    if save:
        insert_df(stock_legu_market_activity_df, 'activities')
    return stock_legu_market_activity_df.set_index('trade_date')


def ak_get_index(indices=['上证指数', '深证成指', '创业板指']):
    stock_zh_index_spot_df = ak.stock_zh_index_spot()
    stock_zh_index_spot_df.columns = ['code', 'name', 'latest', 'amt_chg', 'pct_chg', 'pre_close', 'open', 'high', 'low', 'vol', 'amount']
    return stock_zh_index_spot_df[stock_zh_index_spot_df.name.isin(indices)]

########################################
# From Tencent HTTP
########################################

def tx_auc(symbol=None, ts_code=None):
    summary = '''
    Realtime from Tencent
    '''
    if symbol is None:
        symbol = add_postfix(ts_code=ts_code, type='ak')

    url = "http://stock.gtimg.cn/data/index.php"
    params = {
        "appn": "detail",
        "action": "data",
        "c": symbol,
        "p": 0,
    }
    req = r.get(url, params=params)
    text_data = req.text
    try:
        big_df = (
            pd.DataFrame(eval(text_data[text_data.find("[") :])[1].split("|"))
            .iloc[:, 0]
            .str.split("/", expand=True)
        )
    except:
        print(f'Stock {symbol} has no valid data!')
        big_df = pd.DataFrame()

    if not big_df.empty:
        big_df = big_df.iloc[:, 1:]
        big_df.columns = ["成交时间", "成交价格", "价格变动", "成交量", "成交金额", "性质"]
        big_df.reset_index(drop=True, inplace=True)
        property_map = {
            "S": "卖盘",
            "B": "买盘",
            "M": "中性盘",
        }
        big_df["性质"] = big_df["性质"].map(property_map)
        big_df = big_df.astype({
            '成交时间': str,
            '成交价格': float,
            '价格变动': float,
            '成交量': int,
            '成交金额': int,
            '性质': str,
        })
    return big_df.head(1)


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

def process_plate_res(tmp, name, sleep_counter):
    tmp1 = tmp.rename(columns={'代码':'ts_code'})[['ts_code', 'plate_type']]
    tmp1.ts_code = tmp1.ts_code.apply(add_postfix)
    tmp1['plate_name'] = name
    sleep_counter += 1
    if sleep_counter % 3 == 1:
        sleep(2)
    return tmp1, sleep_counter


if __name__ == '__main__':
    tp = fetch_today_price()
    print(tp)


