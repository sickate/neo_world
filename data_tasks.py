import sys
sys.path.append('./')

from env import *
from sqlbase import engine, db_session, text, desc

from utils.datetimes import daterange, trade_day_util
from utils.argparser import data_params_wrapper
# from utils.datasource import *
from utils.stock_utils import *
from utils.psql_client import load_table, insert_df, get_stock_basic
from utils.datasource import ts, pro, ak, ak_all_plates, ak_today_auctions
from models import *
# from models.daily_basic import DailyBasic
# from models.shibor import Shibor
# from models.stock import Stock

# ts.set_token('5f576fde2efd1ac4df59161bda0a4f04ac599535db9a1ffec1de21de')
# pro = ts.pro_api()


DAILY_MODELS = [DailyBasic, Price, AdjFactor, Money, UpStop]
DATE_MODELS = []


## DATA FETCHING TASKS
@data_params_wrapper
def fetch_ticks(start_date, end_date, verbose=False):
    # 每天的成交数据入库
    for date in daterange(start_date, end_date):
        trade_date = date.strftime("%Y%m%d")
        ts_codes = get_stock(date=trade_date).ts_code.to_list()
        ticks = {}
        for i in trange(len(ts_codes)):
            if i % 20 == 19:
                sleep(1)
            ts_code = ts_codes[i].split('.')[1].lower() + ts_codes[i].split('.')[0]
            tmp = ak.stock_zh_a_tick(code=ts_code, trade_date=trade_date)
            if tmp is not None and len(tmp) > 0:
                tmp.columns = ['tick_at', 'price', 'price_diff', 'volume', 'amount', 'dir']
                ticks[ts_codes[i]] = tmp

        print(f'Start calculate daily volume of {trade_date}')
        x = pd.DataFrame()
        for k, df in ticks.items():
            tmpdict = {}
            buysell = df.groupby('dir').agg({'amount':'sum','volume':'sum'})
            price_vol = df.groupby('price').agg({'volume':'sum'})
            tmpdict['ts_code'] = k
            tmpdict['trade_date'] = trade_date
            tmpdict['high'] = df.price.max()
            tmpdict['low'] = df.price.min()
            tmpdict['close'] = df.iloc[-1,1]
            if '中性盘' in buysell.index:
                tmpdict['neutral_vol'] = buysell.loc['中性盘','volume']
            else:
                tmpdict['neutral_vol'] = 0
            if '买盘' in buysell.index:
                tmpdict['buy_vol'] = buysell.loc['买盘', 'volume']
            else:
                tmpdict['buy_vol'] = 0
            if '卖盘' in buysell.index:
                tmpdict['sell_vol'] = buysell.loc['卖盘', 'volume']
            else:
                tmpdict['sell_vol'] = 0
            tmpdict['max_vol_price'] = np.sum(df.volume * df.price) / df.volume.sum()
            x = x.append(tmpdict, ignore_index=True)

        print(f'Insert to db {trade_date}')
        insert_df(x, Tick.__tablename__)


@data_params_wrapper
def fetch_dragon_jq(start_date, end_date, verbose=False):
    summary = """
    # 接口：JoinQuant Billboard List
    # 数据说明：
    # 调取说明：
    # 描述
    """
    print(summary)
    print(jq.get_query_count())
    start_date = pdl.parse(start_date).to_date_string()
    end_date = pdl.parse(end_date).to_date_string()
    df = jq.get_billboard_list(start_date=start_date, end_date=end_date)
    df.loc[:,'ts_code'] = df.code.map(lambda x: get_ts_code(x))
    df.drop(columns=['code'], inplace=True)
    df.rename(columns={'day':'trade_date', 'change_pct':'pct_chg'}, inplace=True)
    # df.to_sql('', con=engine, if_exists='append', index=False, schema='public')
    insert_df(df, DragonJQ.__tablename__)
    print(f"JoinQuant Billboard of {start_date} to {end_date} saved.")
    print(jq.get_query_count())


@data_params_wrapper
def fetch_ak_auctions(start_date, end_date):
    summary = """
    # 接口：AkShare auction
    # only for last trading day
    """
    ts_codes = get_stock_basic(end_date).index
    ak_today_auctions(ts_codes, save_db=True)



@data_params_wrapper
def fetch_jq_auctions(start_date, end_date, should_save=True, stocks=None, verbose=False):
    summary = """
    # 接口：JoinQuant auction
    """
    print(summary)
    count_before = jq.get_query_count()

    drange = list(daterange(start_date, end_date))
    allcall = pd.DataFrame()
    if stocks is None:
        stocks = jq.get_all_securities().index.to_list()
    for i in trange(len(drange)):
        trade_date = drange[i].strftime("%Y-%m-%d")
        call = jq.get_call_auction(security=stocks, start_date=trade_date, end_date=trade_date)
        if call is None:
            continue
        call.loc[:,'trade_date'] = call.time.map(lambda x: x.date())
        call.loc[:, 'ts_code'] = call.code.map(lambda x: get_ts_code(x))
        call = call.drop(columns=['code', 'time'])

        if len(call) > 0:
            # 单位从股换成手
            call.loc[:,'auc_vol'] = call.volume / 100
            call.drop(columns='volume', inplace=True)
            allcall = allcall.append(call)
            if should_save:
                insert_df(call, Auction.__tablename__)
                print(f"Auctions of {trade_date} are saved.")
    print(f'Before: {count_before}, after: {jq.get_query_count()}')

    if len(allcall) > 0:
        allcall.set_index(['ts_code', 'trade_date'], inplace=True)
    else:
        print(f'Got 0 result from remote server')
    return allcall


@data_params_wrapper
def fetch_money_jq(start_date, end_date, verbose=False):
    summary = """
    # 接口：JoinQuant money_flow
    # 数据说明：
    # 调取说明：
    # 描述
    """
    print(summary)
    print(jq.get_query_count())
    stocks = jq.get_all_securities()
    start_date = pdl.parse(start_date).to_date_string()
    end_date = pdl.parse(end_date).to_date_string()
    df = jq.get_money_flow(stocks.index.to_list(), start_date=start_date, end_date=end_date)
    df.loc[:,'ts_code'] = df.sec_code.map(lambda x: get_ts_code(x))
    df.rename(columns={'date':'trade_date', 'change_pct':'pct_chg'}, inplace=True)
    df.drop(columns=['sec_code'], inplace=True)
    # df.to_sql('', con=engine, if_exists='append', index=False, schema='public')
    insert_df(df, MoneyJQ.__tablename__)
    print(f"MoneyFlow of {start_date} to {end_date}: {len(df)} records are saved.")
    print(jq.get_query_count())


@data_params_wrapper
def fetch_dragon(start_date, end_date, verbose=False):
    """
    # 接口：dragon
    # 数据说明：
    # 调取说明：
    # 描述
    """
    for trade_date in daterange(start_date, end_date):
        print("Getting Dragon Inst of {}".format(trade_date))
        df = pro.top_inst(trade_date=trade_date.strftime("%Y%m%d"))
        if len(df) > 0:
            print(f"Saving Dragon Inst of {trade_date}, total count: {len(df)}")
            df = df.fillna(0)
            df.loc[:,'rank'] = df.groupby(['trade_date','ts_code'])['net_buy'].rank(ascending=False, method='first').apply(lambda x: int(x))
            # df.to_sql('dragon_inst', con=engine, if_exists='append', index=False, schema='public')
            insert_df(df, DragonInst.__tablename__)
            print("Dragon Inst of {} saved.".format(trade_date))

        print("Getting Dragon of {}".format(trade_date))
        df = pro.top_list(trade_date=trade_date.strftime("%Y%m%d"))
        print(f"Saving Dragon of {trade_date}, total count: {len(df)}")
        insert_df(df, Dragon.__tablename__)
        print("Dragon of {} saved.".format(trade_date))


@data_params_wrapper
def fetch_money(start_date, end_date, verbose=False):
    """
    # 接口：money
    # 数据说明：
    # 调取说明：
    # 描述
    """
    for trade_date in daterange(start_date, end_date):
        print("Getting data of {}".format(trade_date))
        df = pro.moneyflow(trade_date=pdl.parse(trade_date).strftime("%Y%m%d"))
        print(f"Saving data of {trade_date}, total count: {len(df)}")
        insert_df(df, Money.__tablename__)
        print("Data of {} saved.".format(trade_date))


@data_params_wrapper
def fetch_prices(start_date, end_date, verbose=False):
    """
    # 接口：daily
    # 数据说明：交易日每天15点～16点之间。本接口是未复权行情，停牌期间不提供数据。
    # 调取说明：基础积分每分钟内最多调取200次，每次4000条数据，相当于超过18年历史，用户获得超过5000积分正常调取无频次限制。
    # 描述：获取股票行情数据，或通过通用行情接口获取数据，包含了前后复权数据。
    """

    for trade_date in daterange(start_date, end_date):
        print("Getting data of {}".format(trade_date))
        df = pro.daily(trade_date=pdl.parse(trade_date).strftime("%Y%m%d"))
        print(f"Saving data of {trade_date}, total count: {len(df)}")
        insert_df(df, Price.__tablename__)
    #     df.to_sql('prices', con=engine, if_exists='append',
    #             index=False, schema='public')
        print("Data of {} saved.".format(trade_date))

def fetch_bk_codes():
    """东方财富所有板块数据
    # 行业板块
    # http://52.push2.eastmoney.com/api/qt/clist/get?pn=1&pz=300&po=1&np=1&fltt=2&invt=2&fid=f3&fs=m:90+t:2&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152,f133,f104,f105&_=1610278846448
    # 概念板块
    # http://52.push2.eastmoney.com/api/qt/clist/get?pn=1&pz=1000&po=1&np=1&fltt=2&invt=2&fid=f3&fs=m:90+t:3&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152,f133,f104,f105&_=1610278846452
    """
    #respone['data']['diff'][]['f12']
    industry_url = 'http://52.push2.eastmoney.com/api/qt/clist/get?pn=1&pz=300&po=1&np=1&fltt=2&invt=2&fid=f3&fs=m:90+t:2&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152,f133,f104,f105&_=1610278846448'
    response = requests.get(industry_url) 
    diff = response.json()['data']['diff']
    industry_codes = list(map(lambda bk: bk['f12'], diff))
    concept_url = 'http://52.push2.eastmoney.com/api/qt/clist/get?pn=1&pz=1000&po=1&np=1&fltt=2&invt=2&fid=f3&fs=m:90+t:3&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152,f133,f104,f105&_=1610278846452'
    response = requests.get(concept_url)
    diff = response.json()['data']['diff']
    concept_codes = list(map(lambda bk: bk['f12'], diff))
    codes = concept_codes + industry_codes
    return { 'concept_codes': concept_codes, 'industry_codes': industry_codes }
    
def fetch_hk_codes():
    """
    东方财富所有股票代码，总数大概4000+
    """
    url = 'http://61.push2.eastmoney.com/api/qt/clist/get?pn=1&pz=10000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:128+t:3,m:128+t:4,m:128+t:1,m:128+t:2&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f19,f20,f21,f23,f24,f25,f26,f22,f33,f11,f62,f128,f136,f115,f152&_=1611581352745'
    response = requests.get(url)
    diff = response.json()['data']['diff']
    return list(map(lambda d: d['f12'], diff))
    
def fetch_hk(start_date=None, end_date=None, trade_date=None, ts_code=None, verbose=False):
    """
        获取港股行情
    """
    date_start = datetime.strptime(start_date, '%Y%m%d')
    date_end = datetime.strptime(end_date, '%Y%m%d')
    lmt = (date_end - date_start).days + 1
    codes = fetch_hk_codes()
    dfhk = pro.hk_basic(list_status='L')
    #codes = dfhk.ts_code.map(lambda x: x[0:5])
    for code in codes:
        sleep(0.1)
        url = f'http://61.push2his.eastmoney.com/api/qt/stock/kline/get?secid=116.{code}&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61&klt=101&fqt=1&end={end_date}&lmt={lmt}'
        response = requests.get(url)
        data = response.json()['data']
        if data is None:
            continue
        cols = ['trade_date', 'open', 'close', 'high', 'low', 'amount', 'money', 'amp', 'pct_chg', 'change', 'turnover_rate']
        #breakpoint()
        klines = [l.split(',') for l in data['klines']]
        df = pd.DataFrame(klines, columns=cols)
        df['ts_code'] = code
        df['name'] = data['name']
        insert_df(df, HkPrice.__tablename__)
        print("Data of {}, {} saved.".format(code, data['name']))


def fetch_bk(start_date=None, end_date=None, trade_date=None, ts_code=None, verbose=False):
    """
    # 获取板块数据
    # 数据源: 东方财富
    """
    #breakpoint()
    codes = fetch_bk_codes()
    all_codes = codes['concept_codes'] + codes['industry_codes']
    for code in all_codes:
        bk_url = f'http://push2his.eastmoney.com/api/qt/stock/kline/get?secid=90.{code}&fields1=f1,f2,f3,f4,f5&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61&klt=101&fqt=0&beg={start_date}&end={end_date}'
        response = requests.get(bk_url + code)
        data = response.json()['data']
        cols = ['trade_date', 'open', 'close', 'high', 'low', 'amount', 'money', 'amp', 'pct_chg', 'change', 'turnover_rate']
        klines = [l.split(',') for l in data['klines']]
        df = pd.DataFrame(klines, columns=cols)
        df['ts_code'] = code
        df['name'] = data['name']
        if code in codes['concept_codes']:
            df['type'] = 'concept'
        else:
            df['type'] = 'industry'
        insert_df(df, BkData.__tablename__)
        print("Data of {},{} saved.".format(code, data['name']))


def fetch_bk_money_flow(start_date=None, end_date=None, trade_date=None, ts_code=None, verbose=False):
    """
    # 获取板块资金数据
    # 数据源: 东方财富
    #breakpoint()
    # "2020-08-14,-154965424.0,171598992.0,-16633552.0,-128603152.0,-26362272.0,-2.51,2.78,-0.27,-2.09,-0.43,1202.12,0.61,0.00,0.00",
    """
    codes = fetch_bk_codes()
    all_codes = codes['concept_codes'] + codes['industry_codes']
    bk_url = 'http://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get?fields1=f1,f2,f3,f7&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63&secid=90.'
    for code in all_codes:
        response = requests.get(bk_url + code)
        data = response.json()['data']
        cols = ['trade_date', 'main_flow', 'sm_flow', 'md_flow', 'lg_flow', 'ultra_flow', 'main_flow_pct', 'sm_flow_pct', 'md_flow_pct', 'lg_flow_pct', 'ultra_flow_pct', 'close', 'pct_chg']
        print(data['klines'])
        klines = [l.split(',') for l in data['klines']]
        #breakpoint()
        df = pd.DataFrame(klines, columns=cols)
        df['ts_code'] = code
        df['name'] = data['name']
        if code in codes['concept_codes']:
            df['type'] = 'concept'
        else:
            df['type'] = 'industry'
        insert_df(df, BkMoneyFlow.__tablename__)
        print("Data of {}, {} Money Flow saved.".format(code, data['name']))


@data_params_wrapper
def fetch_daily_basic(start_date, end_date, verbose=False):
    """
    更新时间：交易日每日15点～17点之间
    描述：获取全部股票每日重要的基本面指标，可用于选股分析、报表展示等。
    积分：用户需要至少300积分才可以调取，具体请参阅积分获取办法

    """

    for trade_date in daterange(start_date, end_date):
        print("Getting data of {}".format(trade_date))
        df = pro.daily_basic(trade_date=pdl.parse(trade_date).strftime("%Y%m%d"))
        print(f"Saving data of {trade_date}, total count: {len(df)}")
        insert_df(df, DailyBasic.__tablename__)
        # df.to_sql('daily_basic', con=engine, if_exists='append',
        #         index=False, schema='public')
        print("Data of {} saved.".format(trade_date))


@data_params_wrapper
def fetch_adj_factor(start_date, end_date, verbose=False):
    for trade_date in daterange(start_date, end_date):
        print("Getting data of {}".format(trade_date))
        df = pro.adj_factor(trade_date=pdl.parse(trade_date).strftime("%Y%m%d"))
        print(f"Saving data of {trade_date}, total count: {len(df)}")
        print(f"Saving data of {trade_date}, total count: {len(df)}")
        insert_df(df, AdjFactor.__tablename__)
        # df.to_sql('adj_factor', con=engine, if_exists='append',
        #         index=False, schema='public')
        print("Data of {} saved.".format(trade_date))



@data_params_wrapper
def fetch_upstops(start_date, end_date, verbose=False):
    for trade_date in daterange(start_date, end_date):
        print("Getting upstops of {}".format(trade_date))
        df = pro.limit_list(trade_date=pdl.parse(trade_date).strftime("%Y%m%d"))
        print(f"Saving upstops of {trade_date}, total count: {len(df)}")
        insert_df(df, UpStop.__tablename__)
        # df.to_sql('adj_factor', con=engine, if_exists='append',
        #         index=False, schema='public')
        print(f"UpStop data of {start_date} to {end_date}, {len(df)} records are saved.")


@data_params_wrapper
def fetch_adj_price(start_date, end_date, ts_code=None, verbose=True):
    should_skip = True
    if ts_code:
        df = ts.pro_bar(ts_code=ts_code, adj='qfq', start_date=start_date, end_date=end_date)
        insert_df(df, AdjPrice.__tablename__)
        if verbose:
            print("Data of {} saved.".format(ts_code))
        return df
    else:
        for ts_code_tuple in db_session.query(Price.ts_code).distinct():
            ts_code = ts_code_tuple[0]
            if verbose:
                print("Getting data of {}".format(ts_code))
            df = ts.pro_bar(ts_code=ts_code, adj='qfq', start_date=start_date, end_date=end_date)
            # if df is not None:
                # df.to_sql('adj_prices', con=engine, if_exists='append',
                    # index=False, schema='public')
                # print("Data of {} saved.".format(ts_code))
            # else:
            #     print("No qfq data for {}".format(ts_code))

            # 防止出现 duplicate issue，一条一条插入
            insert_df(df, AdjPrice.__tablename__)
            if verbose:
                print("Data of {} saved.".format(ts_code))


@data_params_wrapper
def fetch_shibors(start_date, end_date, verbose=False):
    print("Getting Shibor data from {} to {}".format(start_date, end_date))
    df = pro.shibor(start_date=start_date, end_date=end_date)
    print(f"Saving data from {start_date} to {end_date}, total count: {len(df)}")
    insert_df(df, Shibor.__tablename__)
    # df.to_sql('shibors', con=engine, if_exists='append',
    #         index=False, schema='public')
    print("SHIBOR Data from {} to {}".format(start_date, end_date))


@data_params_wrapper
def fetch_stock_shares(start_date, end_date, verbose=False):
    summary = """
    # 接口：JoinQuant Top10 share holder and stock share updates
    # 数据说明：
    # 调取说明：
    # 描述
    """
    print(summary)
    print(jq.get_query_count())
 
    stock_list_jq, stock_list_all_jq, stock_all_jq = fetch_stock_list(today_date=end_date)
    stock_list_all = list(map(get_ts_code, stock_list_all_jq))

    print(f"Start getting stock share data of {start_date} - {end_date}.")

    # fetch total trade share
    dftradeshare = fetch_share_update(start_date, end_date)
    if verbose:
        print(f"Fetched {len(dftradeshare)} stock share updates.")
    # fetch top 10 holders of trade share
    dfsum, dfshare = fetch_trade_share_update(start_date, end_date)
    if len(dfsum) == 0:
        print("No Stock share update today. Skip following process.")
        return None
    if verbose:
        print(f"Fetched {len(dfsum)} top 10 share holders updates.")
    dfsum.loc[:, 'top10_trade_share'] = dfsum.share_number/10000
    dfsum.drop(columns=['share_number'], inplace=True)

    # Join, 去掉 b 股 ts_code
    df_trade_share_update = dfsum.join(dftradeshare, how='outer').sort_index()
    df_trade_share_update = df_trade_share_update[df_trade_share_update.index.get_level_values(0).isin(stock_list_all)]

    print("Saving data to PG...")
    insert_df(df_trade_share_update.reset_index(), StockShare.__tablename__)
    return df_trade_share_update


@data_params_wrapper
def show_data_status():
    for model in DAILY_MODELS:
        if hasattr(model, 'trade_date'):
            date_attr = 'trade_date'
            start = db_session.query(model).order_by('trade_date').first()
            end = db_session.query(model).order_by(text('trade_date desc')).first()
        else:
            date_attr = 'date'
            start = db_session.query(model).order_by('date').first()
            end = db_session.query(model).order_by(text('date desc')).first()

        if start:
            print(f'{model.__name__} starts from {getattr(start, date_attr)}')
            print(f'{model.__name__} ends to {getattr(end, date_attr)}')
            print('')
        else:
            print('No {model.__name__} records.')

#     q = db_session.query(Price).order_by('trade_date')
    # q_desc = db_session.query(Price).order_by(text('trade_date desc'))
    # result = q.first()
    # if result:
        # print(f'Price starts from {result.trade_date}')
        # print(f'Price ends to {q_desc.first().trade_date}')
    # else:
#         print('No Price records.')

    # print
    # q = db_session.query(Price).order_by("trade_date desc").limit(1)
    # if len(result) >= 1:
        # print(result[-1].trade_date)
    # else:
        # print('No Price records.')
    # print(Price.order_by(Price.trade_date.desc()).first().trade_date)
    # print(Price.trade_date.desc().first().trade_date)
    # print(Price.trade_date.first().trade_date)
    # print(DailyBasic.trade_date.desc().first().trade_date)
    # print(DailyBasic.trade_date.first().trade_date)


def fetch_index_infos():
    df = pro.index_basic(market='SW')


def get_basics(trade_date):
    # basics = ts.get_stock_basics() # 获取基本面数据
    # basics
    pass


@data_params_wrapper
def fetch_stock_basics():
    """
    接口：stock_basic
    描述：获取基础信息数据，包括股票代码、名称、上市日期、退市日期等
    """
    fields = 'ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs'
    df = pro.stock_basic(fields=fields).set_index('ts_code')
    akstk = ak.stock_zh_a_spot_em()
    for i in akstk.index:
        akstk.loc[i, 'ts_code'] = add_postfix(akstk.loc[i, '代码'], type='ts')
    aknames = akstk[['ts_code', '名称']].set_index('ts_code').rename(columns={'名称': 'name'})
    df = df.drop(columns='name').join(aknames).reset_index()
    df.to_sql('stock_basic', con=engine, if_exists='replace', index='id', schema='public')
    print("StockBasic is updated.")


@data_params_wrapper
def show_daily_profits(ts_codes=None, buy_prices=None, buy_shares=None):
    if ts_codes is None:
        ts_codes=['sh','hs300', '399001','300661', '600777','300138', '603367','603557', '600016']
    if buy_prices is None:
        buy_prices=[0, 0, 0, 251.741, 2.181, 7.792, 18.897, 8.763, 5.763]
    if buy_shares is None:
        buy_shares=[0, 0, 0, 200, 6300, 2200, 800, 1600, 1800]
        print(min_count)

    df = ts.get_realtime_quotes(ts_codes)
    df['rate']=np.round((df.price.apply(float)/df.pre_close.apply(float) - 1)*100, 2)
    df['hold'] = buy_shares
    df['profit']=(df.price.apply(float) - df.pre_close.apply(float)) * buy_shares
    df['value']=df.price.apply(float) * buy_shares
    df['basic']= df.hold * buy_prices
    df['total_profit']= (df.price.apply(float) - buy_prices) * df.hold
    outputdf = df[['code','name','rate', 'price', 'profit', 'total_profit', 'bid','ask','time']]
    print(outputdf)
    print(f'Total Basic: {df.basic.sum()}')
    print(f'Total Profit: {df.total_profit.sum()}')
    print(f'Current Profit: {df.profit.sum()}')
    print(f'Current Value: {df.value.sum()}')


# 获取 tushare 的指数数据，主要包括当日大盘成交量等
def fetch_index(start_date, end_date):
    for trade_date in tqdm(list(daterange(start_date, end_date))):
        tmp = pro.index_dailybasic(trade_date=trade_date.strftime("%Y%m%d"))
        tmp.loc[:,'trade_date'] = tmp.trade_date.apply(lambda x: pdl.parse(x).to_date_string())
        insert_df(tmp, 'index_daily_basic')


@data_params_wrapper
def check_data_integrity(start_date=None, end_date=None, tables=None, try_fix=True, sleep_time=2):
    from neo import data_tasks
    if end_date is None:
        from utils.datetimes import end_date as last_trade_date
        # from utils.env_constants import end_date as last_trade_date
        end_date = last_trade_date
    if start_date is None:
        start_date = trade_day_util.past_trade_days(days=30)[0]
    days_to_check = trade_day_util.trade_days_between(start_date=start_date, end_date=end_date)
    print(f'Checking days from {days_to_check[0]} to {days_to_check[-1]}, {len(days_to_check)} days in total.')
    # days_to_check = list(filter(lambda d: d.isoformat() >= start_date and d.isoformat() <= end_date, all_trade_day))
    if tables is None:
        # tables = [Price, DailyBasic, Auction, AdjFactor, Money, MoneyJQ, UpStop, Dragon, DragonJQ, StockShare]
        tables = [Price, DailyBasic, AdjFactor, Money, UpStop]
    for tbl in tables:
        df = load_table(tbl, start_date=start_date, end_date=end_date)
        print(f"Checking {tbl.__tablename__}...")
        for day in tqdm(days_to_check):
            tmp_count = len(df.xs(slice(day, day), level=1, drop_level=False))
            min_count = expected_count_in_day(tbl, day)
            if tmp_count < min_count:
                print(f"WARNING: {tbl.__tablename__} [{day}]. Row counts: {tmp_count}, expected: {min_count}.")
                if try_fix:
                    print("Trying fix...")
                    data_tasks(tbl.__tablename__)(start_date=day, end_date=day)
                    sleep(sleep_time)
                    print(f"Fixed: {tbl.__tablename__}: [{day}]. You might want to re-run this check after all done.")
        print(f"Done check {tbl.__tablename__}, got {len(df)} records.")

    df = load_table(Auction, start_date=end_date, end_date=end_date)
    if len(df) < expected_count_in_day(Auction, day):
        data_tasks('auction')(start_date=end_date, end_date=end_date)

    print("Updating StockBasic data...")
    fetch_stock_basics()

    print("Updating plates data...")
    ak_all_plates(use_cache=False)
    return None


def expected_count_in_day(model, date):
    if date > '2022-05-01':
        base_count = 4550
    if date > '2021-11-11':
        base_count = 4400
    if date > '2020-12-01':
        base_count = 4000
    elif date > '2020-01-01':
        base_count = 3700
    elif date > '2019-01-01':
        base_count = 3400
    elif date > '2018-01-01':
        base_count = 3100
    else:
        base_count = 1600

    if model in [UpStop, Dragon]:
        return 20
    elif model in [Money, Auction]:
        from utils.datetimes import week_ago_date
        return len(load_table(Price, week_ago_date, week_ago_date)) - 140
    else:
        return base_count


def color_val(val):
    """
    Takes a scalar and returns a string with
    the css property `'color: red'` for negative
    strings, black otherwise.
    """
    if val > 0:
        color = 'red'
    if val < 0:
        color = 'green'
    else:
        color = 'white'
    return 'color: %s' % color


def bulkload_csv_data_to_database(engine, tablename, columns, data, sep=","):
    logging.info("Start ingesting data into postgres ...")
    logging.info("Table name: {table}".format(table=tablename))
    logging.info("CSV schema: {schema}".format(schema=columns))
    conn = engine.connect().connection
    cursor = conn.cursor()
    cursor.copy_from(data, tablename, columns=columns, sep=sep, null='null')
    conn.commit()
    conn.close()
    logging.info("Finish ingesting")


# samples 

# news
# df = pro.cctv_news(date='20181211')
# df = pro.news(src='sina', start_date='20181121', end_date='20181122')
# 获取单个股票公告数据
# df = pro.anns(ts_code='002149.SZ', start_date='20190401', end_date='20190509', year='2019')
# 获取2019年最新的50条公告数据
# df = pro.anns(year='2019')

