"""
对 ak 和 ts 的封装，取数据
"""

import sys
import requests as r
import json
from os import stat
from os.path import exists, dirname, abspath

sys.path.append('./')
from env import np, pd, pdl, trange, tqdm, display, sleep
from sqlbase import engine

from utils.datetimes import end_date
from utils.psql_client import insert_df, read_pg, get_stock_basic
from utils.stock_utils import *
from utils.stock_filter import StockFilter
from utils.logger import logger

AK_TICK_COLUMNS = ['time', 'price', 'change', 'volume', 'amount', 'direction']
MAJOR_INDICES = ['上证指数', '深证成指', '创业板指', '科创50', '中小综指', '创业板综', '上证50', '沪深300', '中证500', '中证1000']

import akshare as ak
import tushare as ts
from py_mini_racer import py_mini_racer
from akshare.datasets import get_ths_js
from akshare.stock_feature.stock_wencai import _get_file_content_ths

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
# AkShare Today Data ( Must be fetched in day)
#####################################################################

def ak_activity(save=False, verbose=False):
    summary = """
        AkShare 获取当天实时活跃度
    """
    stock_legu_market_activity_df = ak.stock_market_activity_legu().set_index('item').transpose()
    stock_legu_market_activity_df['活跃度'] = stock_legu_market_activity_df['活跃度'].apply(lambda x: float(x.split('%')[0]))
    if verbose:
        display(stock_legu_market_activity_df)
    cols = ['up', 'upstop','real_upstop', 'st_upstop', 'dn', 'dnstop', 'real_dnstop', 'st_dnstop', 'fl', 'halt', 'vitality', 'trade_date']
    stock_legu_market_activity_df.columns = cols
    stock_legu_market_activity_df.trade_date = stock_legu_market_activity_df.trade_date.apply(lambda x: x.split(' ')[0])
    # stock_legu_market_activity_df.vitality = stock_legu_market_activity_df.vitality.apply(lambda x: float(x.split('%')[0]))
    for col in cols[0:-2]:
        stock_legu_market_activity_df[col] = stock_legu_market_activity_df[col].apply(lambda x: int(x))
    if save:
        insert_df(stock_legu_market_activity_df, 'activities')
    return stock_legu_market_activity_df.set_index('trade_date')


def ak_today_index():
    stock_zh_index_spot_df = ak.stock_zh_index_spot()
    stock_zh_index_spot_df.columns = ['code', 'name', 'close', 'amt_chg', 'pct_chg', 'pre_close', 'open', 'high', 'low', 'vol', 'amount']
    stock_zh_index_spot_df.loc[:, 'ts_code'] = stock_zh_index_spot_df.code.map(lambda x: f'{x[2:8]}.{x[0:2].upper()}')
    stock_zh_index_spot_df.drop(columns=['code', 'pre_close'], inplace=True)
    return stock_zh_index_spot_df


def ak_stock_basics(savedb=False):
    '''
    Get latest stock list every day and save them to db
    '''
    # get old StockBasic from db
    stk_basic = get_stock_basic()
    stk_basic.drop(columns='id', inplace=True)
    logger.info(f'Got {len(stk_basic)} StockBasic records from db, start updating...')

    latest_date = pdl.parse(stk_basic.list_date.max()).to_date_string()
    if latest_date < end_date:

        # get all new stocks from ak
        new_df = ak.stock_zh_a_new_em()
        new_df.columns = ['no', 'code', 'name', 'close', 'pct_chg', 'change', 'vol', 'amount', 'amp', 'high', 'low', 'open', 'pre_close', 'vol_ratio', 'turnover_rate', 'ttm', 'pb']
        new_df.loc[:,'ts_code'] = get_ts_code_column(new_df)
        new_df1 = new_df[['ts_code','name']].set_index('ts_code')

        # 获取不在 db 中的新股
        new_stks = subtract(new_df1.index.to_list(), stk_basic.index.to_list())
        # 获取 db 中的次新股
        cnew_stks = stk_basic[stk_basic.name.str.startswith('N') | stk_basic.name.str.startswith('C')].index.to_list() + new_stks

        for ts_code in cnew_stks:
            code = ts_code.split('.')[0]
            basic = ak.stock_individual_info_em(symbol=code).transpose()
            basic = basic.drop(index='item')
            basic.columns = ['total_mv','circ_mv','industry','list_date', 'symbol', 'name', 'total_share', 'float_share']
            basic.loc[:, 'ts_code'] = get_ts_code_column(basic, 'symbol')

            basic1 = basic.set_index('ts_code')[['industry','name','symbol','list_date']]
            logger.info(f'Start updating {ts_code} {basic1.loc[ts_code, "name"]} to StockBasic')
            try:
                stk_basic = pd.concat([stk_basic, basic1], verify_integrity=True)
            except ValueError as ve:
                stk_basic.loc[ts_code, 'name']=basic1.loc[ts_code, 'name']

        if savedb:
            logger.info(f'Saving {len(stk_basic)} StockBasic records to db...')
            stk_basic.reset_index().to_sql('stock_basic', con=engine, if_exists='replace', index='id', schema='public')

    stk_basic.loc[:,'list_date'] = stk_basic.list_date.map(str).apply(lambda x: x[0:4]+'-'+x[4:6]+'-'+x[-2:])
    return stk_basic


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
        if updated_on >= pdl.today().date() and not major_update:
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
                    tmp = ak_get_cons_ths(name)
                    # try:
                        # tmp = ak.stock_board_concept_cons_ths(symbol=name)
                    # except ValueError as ve:
                        # sleep(sleep_counter * 5)
                    #     tmp = ak.stock_board_concept_cons_ths(symbol=name)
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
            con_df = con_df.reset_index().drop(columns=['level_0'], errors='ignore')
            con_df.to_feather(cons_file)
            print(f'{len(con_df)} plates are loaded.')
            return con_df


def ak_get_cons_ths(name):
    sleep_counter = 0
    while True:
        try:
            tmp = ak.stock_board_concept_cons_ths(symbol=name)
            break
        except ValueError as ve:
            print(f'Got Value Error on {name}, sleep {sleep_counter * 5}')
            sleep(sleep_counter * 5)
            sleep_counter += 1
    return tmp


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
    res = []
    if open_mkt:
        ind = 11
    else:
        ind = 252
        save_db = False
    error_codes = []
    for ts_code in tqdm(ts_codes):
        code = ts_code.split('.')[0]
        mkt = ts_code.split('.')[1]
        if mkt == 'BJ': # skip 北交所
            continue
        try:
            stock_zh_a_hist_pre_min_em_df = ak.stock_zh_a_hist_pre_min_em(symbol=code)
            stock_zh_a_hist_pre_min_em_df.columns = ['time', 'open', 'close', 'high', 'low', 'auc_vol', 'auc_amt', 'latest']
            stock_zh_a_hist_pre_min_em_df.loc[:,'ts_code'] = ts_code
            # res = res.append(stock_zh_a_hist_pre_min_em_df.loc[ind])
            res.append(stock_zh_a_hist_pre_min_em_df.iloc[ind:ind+1])
        except Exception as e:
            logger.error(e)
            error_codes.append(add_postfix(code))
            logger.error(f'Trying get {code} auction data got error.')

    res = pd.concat(res)
    res.loc[:,'trade_date'] = res['time'].apply(lambda t: t.split(' ')[0])
    res.set_index(['ts_code', 'trade_date'], inplace=True)

    if save_db:
        today = res.reset_index()['trade_date'].at[0]
        yesterday = tdu.past_trade_days(end_date=today)[-2]
        pre_price = load_table(model=Price, start_date=yesterday, end_date=yesterday)[['close']].droplevel('trade_date').rename(columns={'close':'pre_close'})
        auctions = res.join(pre_price)
        auctions.loc[:,'open_pct']=round((auctions.open/auctions.pre_close-1) * 100, 2)
        auctions = auctions[['open', 'pre_close', 'open_pct', 'auc_vol', 'auc_amt']].reset_index()
        insert_df(df=auctions, tablename='auction')

    if len(error_codes) > 0:
        error_res = ak_today_auctions(error_codes, save_db=save_db, open_mkt=open_mkt)
        res = pd.concat([res, error_res])
    return res

#####################################################################
# AkShare History Data ( Must provide a date)
#####################################################################


def ak_daily_prices(tdate):
    '''
    Datasource:
    * stock_zh_a_hist: 东财的日线数据
    Output:
    * adj_factor table: 根据昨日和今日比较，计算需要更新的 adj_factor
    * price:当日的日线数据
    * daily_basic: 当日的股本等数据
    '''
    t1date = tdu.past_trade_days(end_date=tdate, days=2)[0]
    tdate_slim = tdate.replace('-', '')
    t1date_slim = t1date.replace('-', '')

    # 从上一日复制adj数据
    adj_factor = load_table(model=AdjFactor, start_date=t1date, end_date=t1date)
    adj_factor_new = adj_factor.reset_index()
    adj_factor_new.trade_date = tdate
    adj_factor_new.set_index(['ts_code','trade_date'], inplace=True)

    # 加上本日新股 adj 数据，设置 adj_factor 为 1
    stk_basic = get_stock_basic(tdate)
    for new_stock_ts_code in stk_basic[stk_basic.list_date==tdate_slim].index:
        logger.info(f'Adding new stock {stk_basic.loc[new_stock_ts_code, "name"]} in {tdate}')
        adj_factor_new.loc[(new_stock_ts_code, end_date),:] = (1)

    missing_stk = subtract(stk_basic.index.to_list(), adj_factor_new.index.get_level_values('ts_code'))
    if len(missing_stk) > 0:
        logger.warn(f'Stock {missing_stk} is missed in {t1date} AdjFactor')

    prices = []
    paused_stocks = []

    for symbol in tqdm(stk_basic.symbol):
        ts_code = add_postfix(symbol)
        # get prices
        bfq = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=t1date_slim, end_date=tdate_slim, adjust="")
        qfq = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=t1date_slim, end_date=tdate_slim, adjust="qfq")
        if len(qfq) == 0: #
            logger.warn(f'Stock {ts_code} has no record in {t1date} and {tdate}. skip...')
            paused_stocks.append(ts_code)
            continue

        qfq.columns = ['trade_date', 'open', 'close', 'high', 'low', 'vol', 'amount', 'amp', 'pct_chg', 'change', 'turnover_rate']
        bfq.columns = ['trade_date', 'open', 'close', 'high', 'low', 'vol', 'amount', 'amp', 'pct_chg', 'change', 'turnover_rate']

        # 将当日开市票的当日价格 df 取出加入数组
        qfq.loc[:,'ts_code'] = ts_code
        price = qfq.set_index('trade_date')
        if len(price[price.index==tdate])>0:
            prices.append(price[price.index==tdate])

        # check fq
        if len(qfq)==2:
            if (bfq.loc[0, 'close'] != qfq.loc[0, 'close']):
                fq_ratio = bfq.loc[0, 'close'] / qfq.loc[0, 'close']
                adj_factor_new.loc[(ts_code, tdate), 'adj_factor'] = round(fq_ratio * adj_factor_new.loc[(ts_code, tdate), 'adj_factor'], 4)
                logger.info(f'Stock {symbol} has 除权 on {tdate}, the ratio is {fq_ratio}, and new adj_factor is {adj_factor_new.loc[(ts_code, tdate), "adj_factor"]}')
        else:
            logger.info(f'Stock {ts_code} has no record in {t1date} or {tdate}. skip...')

    # logger.info(f'Saving {tdate} {len(adj_factor_new)} adj_factors to db...')
    # insert_df(adj_factor_new.reset_index(), AdjFactor.__tablename__)

    price_df = pd.concat(prices)
    logger.info(f'Saving {tdate} {len(price_df)} prices to db...')
    price_df.loc[:,'pre_close'] = price_df.close - price_df.change
    return adj_factor_new, price_df, paused_stocks


def ak_daily_basic(tdate):
    '''
    From iWencai HTTP
    '''
    page_size=100
    tdate_slim = tdate.replace('-', '')

    url = 'http://www.iwencai.com/customized/chart/get-robot-data'
    js_code = py_mini_racer.MiniRacer()
    js_content = _get_file_content_ths("ths.js")
    js_code.eval(js_content)
    v_code = js_code.call("v")

    headers = {
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36',
        'hexin-v': v_code,
    }

    df_list = []
    page_num = 1
    punishment_time = 51
    grace_sleep_time = 10

    cols = {
        '总股本': 'total_share',
        '流通a股': 'float_share',
        '自由流通股': 'free_share',
        '自由流通市值': 'free_mv',
        '换手率': 'turnover_rate',
        '实际换手率': 'turnover_rate_f',
        '量比': 'volume_ratio',
        '收盘价:不复权': 'close',
        '250日均线': 'ma_close_250'
    }
    ths_cols = list(map(lambda x: f'{x}[{tdate_slim}]', cols.keys()))

    while True:
        json_data = {
            'question': f'{tdate_slim}收盘价，流通股本，实际流通股本，换手率，实际换手率，量比，ma250',
            'perpage': page_size,
            'page': page_num,
            'secondary_intent': 'stock',
            'log_info': '{"input_type":"click"}',
            'source': 'Ths_iwencai_Xuangu',
            'version': '2.0',
            'query_area': '',
            'block_list': '',
            'add_info': '{"urp":{"scene":1,"company":1,"business":1},"contentType":"json","searchInfo":true}',
            'rsh': 'Ths_iwencai_Xuangu_f3ex4o9cm4754fm1lfjzk5x2e3u8e1ul',
        }

        resp = r.post(url, headers=headers, json=json_data)

        try:
            ans = json.loads(resp.content.decode('utf8'))['data']['answer']
            datas = ans[0]['txt'][0]['content']['components'][0]['data']['datas']
        except Exception as je:
            logger.error(je)
            # punishment_time += random.randint(1,10)
            logger.info(f'Got error in page {page_num}, wait {grace_sleep_time + punishment_time} seconds...')
            sleep(grace_sleep_time + punishment_time)
            continue

        tmpdf = pd.DataFrame(data=datas)
        try:
            tmpdf1 = tmpdf[['股票代码', '股票简称']+ths_cols].copy()
        except Exception as col_e:
            logger.error(f'Wrong column names at page {page_num}')
            logger.error(f'{tmpdf.columns}')
            continue
        tmpdf1.columns = ['ts_code', 'name'] + list(cols.values())
        tmpdf1.loc[:,'trade_date'] = tdate

        df_list.append(tmpdf1.set_index(['ts_code','trade_date']))
        if len(tmpdf) == page_size:
            print('█', end ="")
            page_num += 1
            sleep(grace_sleep_time)
        else:
            break
    res_df = pd.concat(df_list)
    # 从股改为手
    res_df.loc[:, 'total_share'] = res_df.total_share.astype(float) / 100
    res_df.loc[:, 'float_share'] = res_df.float_share.astype(float) / 100
    res_df.loc[:, 'free_share'] = res_df.free_share.astype(float) / 100
    res_df.turnover_rate = res_df.turnover_rate.astype(float)
    res_df.volume_ratio = res_df.volume_ratio.astype(float)
    res_df.ma_close_250 = res_df.ma_close_250.map(lambda x: np.nan if isinstance(x, str) and len(x)==0 else x)
    res_df.ma_close_250 = res_df.ma_close_250.astype(float)
    return res_df


def ak_get_limit(tdate, savedb=False):
    '''
    Get Upstop/Downstop/zhaban stocks
    '''
    tmp_date = tdate.replace('-','')

    raw = {
        'U': ak.stock_zt_pool_em(date=tmp_date),
        'D': ak.stock_zt_pool_dtgc_em(date=tmp_date),
        'Z': ak.stock_zt_pool_zbgc_em(date=tmp_date),
    }

    raw_cols = {
        'U': ['no', 'code', 'name', 'pct_chg', 'close', 'amount', 'circ_mv', 'total_mv', 'turnover_rate', 'fd_amount', 'first_time', 'last_time', 'open_times', 'up_stat', 'conseq_up_num', 'industry'],
        'D': ['no', 'code', 'name', 'pct_chg', 'close', 'amount', 'circ_mv', 'total_mv', 'ttm', 'turnover_rate', 'fd_amount', 'last_time', 'dn_amount', 'conseq_dn_num', 'open_times', 'industry'],
        'Z': ['no', 'code', 'name', 'pct_chg', 'close', 'upstop_price', 'amount', 'circ_mv', 'total_mv', 'turnover_rate', 'pct_chg_speed', 'first_time', 'open_times', 'up_stat', 'amp', 'industry'],
    }

    udz_list = []

    for limit_type, tmp in raw.items():
        if len(tmp) > 0:
            tmp.columns = raw_cols[limit_type]
            tmp.loc[:, 'ts_code']= tmp.code.apply(lambda s: add_postfix(str(s)))
            tmp.loc[:, 'trade_date'] = tdate
            tmp.loc[:, 'limit'] = limit_type
            tmp.drop(columns=['no', 'code', 'upstop_price', 'circ_mv', 'pct_chg_speed', 'total_mv', 'ttm', 'turnover_rate', 'amp'], inplace=True, errors='ignore')
            udz_list.append(tmp)

    udz = pd.concat(udz_list, ignore_index=True).set_index(['ts_code', 'trade_date'])
    if savedb:
        insert_df(df=udz.reset_index(), tablename='limit_stocks')
    return udz



########################################
# Get stock related data from db and akshare
########################################

def get_index(tdate, indices=None, verbose=False):
    stock_zh_index_spot_df = load_table(end_date=tdate, start_date=tdate, model=Index)

    # fetch new if available
    if len(stock_zh_index_spot_df) < 500 and (tdate == end_date):
        stock_zh_index_spot_df = ak_today_index()
        stock_zh_index_spot_df.loc[:, 'trade_date'] = tdate
        if verbose:
            display(stock_zh_index_spot_df)
        insert_df(stock_zh_index_spot_df, 'indices')

    if indices:
        return stock_zh_index_spot_df[stock_zh_index_spot_df.name.isin(indices)]
    else:
        return stock_zh_index_spot_df


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


#####################################################################
# Market Summary Utils (TODO)
#####################################################################

def market_summary(df, upstop_trend_df, end_date):
    today_df = df.xs(end_date, level='trade_date', drop_level=True)
    today_hs = StockFilter(end_date=end_date).hs().filter(today_df)

    total_amt = round(today_hs.amount.sum()/100000, 2)

    try:
        bs_amount = round(ak.stock_hsgt_north_net_flow_in_em(symbol="北上").rename(columns={'date': 'trade_date'}).set_index('trade_date').loc[end_date].value / 10000, 2)
    except:
        bs_amount = None
    try:
        hgt_amount = round(ak.stock_hsgt_north_net_flow_in_em(symbol="沪股通").rename(columns={'date': 'trade_date'}).set_index('trade_date').loc[end_date].value / 10000, 2)
    except:
        hgt_amount = None
    try:
        sgt_amount = round(ak.stock_hsgt_north_net_flow_in_em(symbol="深股通").rename(columns={'date': 'trade_date'}).set_index('trade_date').loc[end_date].value / 10000, 2)
    except:
        sgt_amount = None

    median_pct_chg = round(today_hs.pct_chg.mean(),2)
    emo = read_pg(table='activities')
    emo['trade_date'] = emo.trade_date.apply(lambda x: pdl.parse(x))
    emo.set_index('trade_date', inplace=True)
    emo = emo.loc[end_date]

    #upstops
    today_hs = StockFilter(end_date=end_date).hs().not_st().filter(today_df)
    today_uped = today_hs[today_hs.high == today_hs.upstop_price]
    today_up = today_hs[today_hs.limit=='U']
    today_dn = today_hs[today_hs.limit=='D']
    up_fail_rate = round((len(today_uped)-len(today_up))/len(today_uped)*100,2)

    # trend  pre_up_pct	pre_ups_pct	p_up_t_noup_pct
    pre_up_pct = round(upstop_trend_df.tail(1).pre_up_pct.iat[-1], 2)
    pre_ups_pct = round(upstop_trend_df.tail(1).pre_ups_pct.iat[-1], 2)
    p_up_t_noup_pct = round(upstop_trend_df.tail(1).p_up_t_noup_pct.iat[-1], 2)
    pre_up_cons_pct = round(upstop_trend_df.loc[end_date].pre_up_cons_pct, 2)     # 昨涨停晋级率

    indices_df = get_index(end_date, indices=None, verbose=False)
    if len(indices_df) > 0:
        indices_df = indices_df.reset_index().set_index('name')
        sh = indices_df.loc["上证指数"]
        sz = indices_df.loc["深证成指"]
        cyb = indices_df.loc["创业板指"]
        index_text = f'''
上证指数{round(sh.close,1)}点，{sh.pct_chg}%, 深成指{round(sz.close,1)}点，{sz.pct_chg}%, 创业板指{round(cyb.close,1)}点，{cyb.pct_chg}%
'''
    else:
        index_text = ''

    summary_text = f'''
【{end_date}】
{index_text}
总成交{total_amt}亿，北上总流入{bs_amount}亿（沪{hgt_amount}亿, 深{sgt_amount}亿）。涨跌比：{emo.up.astype("int")}/{emo.dn.astype("int")}，中位涨幅：{median_pct_chg}%，热度：{emo.vitality}。
涨跌停：{emo.real_upstop.astype("int")}/{emo.real_dnstop.astype("int")}，炸板率{up_fail_rate}%，连板高度{today_up.conseq_up_num.max()}板。昨涨停晋级率{pre_up_cons_pct}%，昨涨停平均涨幅{pre_up_pct}%，掉队股平均涨幅{p_up_t_noup_pct}%。
    '''
    return summary_text


########################################
# Helpers
########################################

def process_plate_res(tmp, name, sleep_counter):
    tmp1 = tmp.rename(columns={'代码':'ts_code'})[['ts_code', 'plate_type']]
    tmp1.ts_code = tmp1.ts_code.apply(add_postfix)
    tmp1['plate_name'] = name
    sleep_counter += 1
    if sleep_counter % 3 == 1:
        sleep(3)
    return tmp1, sleep_counter


if __name__ == '__main__':
    tp = fetch_today_price()
    print(tp)


