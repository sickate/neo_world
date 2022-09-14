# Bot 
# Author: tuo

import gc

from env import *
from sqlbase import *

from models import *
from utils.datasource import *
from utils.psql_client import *
from utils.stock_filter import *
from utils.stock_utils import *
from utils.strategy import *
from utils.type_helpers import *
from utils.notifiers import wechat_bot
from utils.logger import *
from utils.argparser import bot_options
from utils.datetimes import all_trade_days, today_date, week_ago_date, next_date, trade_day_util as tdu

from data_center import clean_cache_files
from data_tasks import *

import dataframe_image as dfi

import warnings
warnings.simplefilter(action='ignore')


preview_cols = [
    'name', 'plate_name', 'close', 'ma_close_60', # 'pct_chg',
    'conseq_up_num', 'up_type',
    # 'open_times','strth', 'last_time', 'first_time', 'fc_ratio', 'fl_ratio',
    'circ_mv', 'total_mv', 'turnover_rate_f', 'vol_ratio', 'amount',
    # 'dde', 'dde_amt',
]

preview_noup_cols = [
    'name', 'plate_name', 'close', 'ma_close_60', 'pct_chg',
    'circ_mv', 'total_mv', 'turnover_rate_f', 'vol_ratio', 'amount', 'dde', 'dde_amt',
]

slim_cols = [
    'name', 'plate_name', 'close', 'pct_chg', 'conseq_up_num', 'up_type',
    'circ_mv', 'total_mv', 'turnover_rate_f', 'vol_ratio', 'amount', # 'dde', 'dde_amt',
]
review_cols = slim_cols + ['next_auc_amt', 'next_auc_pvol_ratio', 'next_open_pct', 'next_pct_chg']

auc_preview_cols = preview_cols + ['next_auc_amt', 'next_auc_pvol_ratio', 'next_open_pct']

notification_cols = [
    'name', 'plate_name', 'close', 'conseq_up_num',
    'circ_mv', 'turnover_rate_f', 'vol_ratio', 'amount'
]

avg_p_cols = ['name', 'plate_name', 'pre_limit', 'limit', 'pre10_upstops', 'vol_ratio', 'open', 'close', 'pct_chg', 'avg_price', 'ma_close_5', 'circ_mv', 'turnover_rate_f', 'amount']

def refine_variables(df, stra):
    rules = stra.rules
    for k, v in rules.items():
        if k not in df:
            logger.warn(f'Var {k} is not there, generating...')


class Bot():

    def __init__(self, start_date, end_date, task_name):
        logger.info(f'Start working on [{start_date} - {end_date}] Task: {task_name}... ')
        self.end_date = end_date

        # prepare stock data
        self.df = slim_init(start_date, end_date, 10)
        clean_cache_files()

        # top cons
        if pdl.parse(end_date).weekday() == 5:
            plate_mapping = ak_all_plates(use_cache=False, major_update=True, verbose=False)
        else:
            plate_mapping = ak_all_plates(use_cache=True, major_update=False, verbose=False)

        logger.info(f'Total plate number: {len(plate_mapping)}')
        con_sum, ind_sum, cons_detail = calc_plate_data(
            self.df.xs(slice(week_ago_date, end_date), level='trade_date', drop_level=False),
            plate_mapping
        )
        cons_today = cons_detail.xs(end_date, level='trade_date')
        self.top_cons = calc_top_cons(cons_today=cons_today, cons=con_sum.xs(end_date, level='trade_date', drop_level=True))

        # perform task
        self.task = getattr(self, task_name)


    def perform(self):
        self.task()


    def prep_data(self):
        send_notification(f'{self.end_date} data is prepared. Total {len(self.df)} records.')
        pass


    def before_mkt(self):
        # box
        # box_stra.rules['conseq_up_num'] = [{'op': '=', 'val': 1}] # remove list_days stra
        # logger.debug(box_stra.rules)
        # res, keys = box_stra.get_result(self.df, self.end_date)
        # logger.info(f'Found {len(res)}')
        # res.sort_values('turnover_rate_f', inplace=True)
        # res = res.join(self.top_cons)
        # send_stra_result(res[preview_cols], strategy='BOX')

        # avg_price_deep
        res, keys = avgp_stra2.get_result(self.df, self.end_date)
        res = res.join(self.top_cons)
        if len(res) > 0:
            send_notification(f'[{self.end_date}] Stra {avgp_stra2.name} next open_pct must > -9% and < 0%, and > ma5')
            send_stra_result(res[avg_p_cols])

        # avg_price ori
        res, keys = avgp_stra.get_result(self.df, self.end_date)
        res = res.join(self.top_cons)
        if len(res) > 0:
            send_notification(f'[{self.end_date}] Stra {avgp_stra.name} next open_pct must > -9% and < 0%, and > ma5')
            send_stra_result(res[avg_p_cols])


    def open_mkt(self):
        up_auc_vol_stra = Strategy(name='up & auc', stock_filter=StockFilter(end_date).not_st().tui(anti=True).zb())
        up_stra.rules['conseq_up_num'] = [{'op': '>=', 'val': 1}, {'op': '<=', 'val': 3}] # remove list_days stra
        up_auc_vol_stra.merge_other(up_stra)
        df2, a = up_auc_vol_stra.get_result(df=self.df.drop(columns='next_auc_amt'), trade_date=end_date)
        df2 = df2.join(self.top_cons)
        logger.info(f'Get {len(df2)} records from 1st filter...')

        while True:
            now = pdl.now()
            logger.debug(f'{now.hour}:{now.minute}:{now.second}')
            if now.hour >= 9 and now.minute >= 25 and now.second >= 8:
                logger.info('Start pulling auction data...')
                # 获取竞价数据（Run this after trading day 9:25）
                auc = ak_today_auctions(ts_codes=df2.index)
                auc1 = auc.rename(columns={'auc_amt':'next_auc_amt', 'open':'next_open'}).droplevel('trade_date')[['next_auc_amt','next_open']]

                # 合并竞价数据
                df3 = df2.join(auc1)
                df3.loc[:, 'next_open_pct'] = round((df3.next_open/df3.close-1)*100, 2)
                df3.loc[:, 'next_auc_pvol_ratio'] = round(df3.next_auc_amt/df3.amount, 3)

                # 得到最终数据
                up_auc_vol_stra.merge_other(auc_stra)
                df4, a = up_auc_vol_stra.get_result(df=df3)
                logger.info(f'AUC strategy got {len(df4)} records.')
                send_stra_result(df4[auc_preview_cols].sort_values('next_auc_pvol_ratio', ascending=False), strategy='AUC')
                break
            sleep(5)


    def close_mkt(self):
        # 昨日炸板
        stra_zha=Strategy(name='zha')
        stra_zha.add_condition('high', '=', var='upstop_price')
        stra_zha.add_condition('close', '<', var='upstop_price')
        stra_zha.add_condition('close', '<', var='ma_close_5', ratio=1.05) # 临时改为 1.08，原 1.05
        # 非大阴线，非烂板
        stra_zha.add_condition('close', '<', var='high', ratio=0.99)
        stra_zha.add_condition('open', '<', var='close', ratio=0.99)

        stra_zha.stock_filter = StockFilter(end_date).not_st().tui(anti=True).zb()
        zha_df, a = stra_zha.get_result(df=self.df.drop(columns='next_auc_amt'), trade_date=end_date)
        zha_df = zha_df.join(self.top_cons)
        logger.info(f'Zha Strategy got {len(zha_df)} records at last EOD.')
        logger.debug(df_to_text(zha_df, prefix_newline=True))

        # 获取竞价数据（Run this after trading day 9:25）
        zha_auc = ak_today_auctions(ts_codes=zha_df.index)
        zha_auc = zha_auc.rename(columns={'auc_amt':'next_auc_amt', 'open':'next_open'}).droplevel('trade_date')[['next_auc_amt','next_open']]

        # 合并竞价数据
        zha_df = zha_df.join(zha_auc)
        zha_df.loc[:, 'next_open_pct'] = round((zha_df.next_open/zha_df.close-1)*100, 2)
        zha_df.loc[:, 'next_auc_pvol_ratio'] = round(zha_df.next_auc_amt/zha_df.amount, 3)

        stra_zha.add_condition('next_open_pct', '<', val=-1)

        # 得到最终数据
        zha_df_open, a = stra_zha.get_result(df=zha_df)
        logger.info(f'Zha Strategy got {len(zha_df_open[slim_cols])} records after Open')
        logger.debug(df_to_text(zha_df_open, prefix_newline=True))

        logger.info('Waiting for close market auction data...')
        while True:
            sleep(5)
            now = pdl.now()
            if now.hour >= 14 and now.minute >= 57 and now.second >= 10:
                # 获取收盘前数据
                zha_close = ak_today_auctions(ts_codes=zha_df_open.index, open_mkt=False)
                zha_close = zha_close.rename(columns={'close':'next_close'}).droplevel('trade_date')[['next_close']]

                # 合并竞价数据
                zha_df_close = zha_df_open.join(zha_close)
                zha_df_close.loc[:, 'next_pct_chg'] = round((zha_df_close.next_close/zha_df_close.close-1)*100, 2)

                stra_zha.add_condition('next_pct_chg', '<', val=-5)
                stra_zha.add_condition('next_pct_chg', '>', val=-9)

                # 得到最终数据
                zha_df_res, a = stra_zha.get_result(df=zha_df_close)
                logger.info(f'Zha strategy got {len(zha_df_res)} records.')
                send_stra_result(zha_df_res[slim_cols], strategy='Zha')
                break

def slim_init(start_date, end_date, expire_days=30):
    print(f'{ROOT_PATH}/tmp/priceslim_{start_date}_{end_date}_*.feather')
    search_pattern = glob.glob(f'{ROOT_PATH}/tmp/priceslim_{start_date}_{end_date}_*.feather')
    for f in search_pattern:
        # read cache
        logger.info(f'Found cache file: {f}, loading...')
        df = pd.read_feather(f).set_index(['ts_code', 'trade_date'])
        break
    else:
        logger.info(f'No cache found. Start processing...')

        print(f'Initializing data from {start_date} to {end_date}...')
        dc = DataCenter(start_date, end_date)

        stk_basic = dc.get_stock_basics()
        logger.debug(f'stk_basic Memory: {stk_basic.memory_usage(deep=True)}')

        logger.info(f'Start processing price df...')
        price  = dc.get_price()
        logger.debug(f'price Memory: {price.memory_usage(deep=True)}')

        logger.info('Processing Upstop data...')
        upstop = dc.get_upstops(slim=True)
        upstop.loc[:, 'upstop_num'] = upstop.limit.map(lambda lim: 1 if lim == 'U' else -1 if lim =='D' else 0)

        logger.info('Processing auction data...')
        auctions = dc.get_auctions()[['auc_vol', 'auc_amt']]
        logger.debug(f'auction Memory: {auctions.memory_usage(deep=True)}')

        logger.info(f'Join price with other columns...')

        df = (
            stk_basic[['name', 'list_date']].astype(dtype="string[pyarrow]").join(price).join(upstop).join(auctions)
        )
        logger.debug(f'{len(df)} df Memory after join: {df.memory_usage(deep=True)}')

        # fix limit type
        df.limit.fillna('N', inplace=True)
        df.loc[:, 'limit'] = df['limit'].astype(dtype="string[pyarrow]")
        logger.debug(f'{len(df)} df Memory after pyarrow limit type: {df.memory_usage(deep=True)}')

        # calc avg_p
        df.loc[:, 'vol_ratio'] = round(df.vol / df.ma_vol_5, 2)
        df.loc[:, 'avg_price'] = round(df.amount / df.adj_vol / 100, 2)
        df.loc[:, 'avg_profit'] = (df.close-df.avg_price)/df.avg_price * df.amount # 当日获利资金金额
        df.loc[:, 'open_pct'] = round((df.open/df.pre_close-1)*100, 2)
        df.loc[:, 'cvo'] = df.pct_chg - df.open_pct

        # 'circ_mv', 'total_mv'
        df.loc[:, 'circ_mv'] = df.float_share * df.close * 100
        df.loc[:, 'total_mv'] = df.total_share * df.close * 100

        df.drop(columns=[
            'change', 'adj_factor', 'total_share', 'float_share',
            'free_share', 'last_factor', 'norm_adj', 'adj_vol',
            # 'pe', 'pe_ttm', 'amp',
        ], inplace=True)

        logger.info(df.columns)
        logger.debug(f'{len(df)} df Memory after join: {df.memory_usage(deep=True)}')

        df = StockFilter(end_date).tui(anti=True).not_st().filter(df)
        logger.debug(f'{len(df)} df Memory after join: {df.memory_usage(deep=True)}')

        logger.info('Filling upstops..')
        df.upstop_num.fillna(0, inplace=True)
        df.conseq_up_num.fillna(0, inplace=True)

        # calc upstops
        df.loc[:, 'upstop_price'] = df.pre_close.apply(up_stop_price)
        # 累计连续涨停个数
        df.loc[:, 'up_type'] = df[df.limit=='U'].apply(f_set_upstop_types, axis=1).astype(dtype="string[pyarrow]")
        df.loc[:, 'pre5_upstops'] = df.groupby(level='ts_code').upstop_num.apply(lambda x: x.rolling(window=5, min_periods=1).sum()).astype('int8')
        df.loc[:, 'pre10_upstops'] = df.groupby(level='ts_code').upstop_num.apply(lambda x: x.rolling(window=10, min_periods=1).sum()).astype('int8')
        logger.debug(f'df Memory: {df.memory_usage(deep=True)}')

        # 计算 bar type
        df.loc[:, 'bar_type'] = df.apply(f_calc_yinyang, axis=1)
        df.loc[:, 'pre2_bar_type'] = df.groupby('ts_code').bar_type.shift(2)

        logger.info(f'Start processing complex price...')
        df.loc[:, 'pre_close_6'] = df.groupby(level='ts_code').close.shift(6)
        df.loc[:, 'pre_close_21'] = df.groupby(level='ts_code').close.shift(21)
        df.loc[:, 'pre_pct_chg'] = df.groupby(level='ts_code').pct_chg.shift(1)
        df.loc[:, 'pre5_pct_chg'] = (df.pre_close/df.pre_close_6 - 1) * 100
        df.loc[:, 'pre20_pct_chg'] = (df.pre_close/df.pre_close_21 - 1) * 100
        logger.debug(f'df Memory: {df.memory_usage(deep=True)}')

        # 计算 list_days
        print('Calculating list_days...')
        df['cur_date'] = df.index.get_level_values('trade_date').map(lambda x: x.strftime('%Y-%m-%d'))
        tmp = df.loc[df.groupby('ts_code').head(1).index]
        tmp['list_days'] = tmp.cur_date.map(lambda x: tdu.past_trade_days().index(x)) - \
                           tmp.list_date.map(lambda x: tdu.past_trade_days().index(x)) + 1
        df['list_days'] = tmp['list_days']
        df.list_days.fillna(1, inplace=True)
        df.list_days = df.groupby('ts_code').list_days.cumsum()


        print('Performing shift to get prev signals...')
        for ind in ['open_times', 'amount', 'vol', 'vol_ratio', 'open_times', 'up_type', 'conseq_up_num', 'bar_type', 'limit', 'pre10_upstops', 'avg_price', 'pct_chg']:
            df.loc[:, f'pre_{ind}'] = df.groupby(level='ts_code')[ind].shift(1)
        for ind in ['cvo', 'auc_amt', 'auc_vol', 'bar_type', 'vol_ratio', 'open_pct', 'up_type', 'limit', 'pct_chg']:
            df.loc[:, f'next_{ind}'] = df.groupby(level='ts_code')[ind].shift(-1)

        df.loc[:, 'next_auc_pvol_ratio'] = df.next_auc_amt/df.amount
        logger.debug(f'df Memory: {df.memory_usage(deep=True)}')

        # calc y sigs
        # 次日开盘 v 今日开盘（开盘买，次日开盘卖)
        # df_init.loc[:, 'next_open_v_open'] = round((df_init.next_open/df_init.open-1)*100,2)
        # 次2日开盘 v 次日开盘（明日开盘买，次2日开盘卖)
        # df_init.loc[:, 'next2_open_v_open'] = df_init.groupby(level='ts_code').next_open_v_open.shift(-1)

        logger.info('Calculating MAs...')
        df = gen_ma(df, col='close', mavgs=[5, 10, 30, 60])
        logger.debug(f'df Memory: {df.memory_usage(deep=True)}')

        # Max in prev days
        for span in [10, 60]:
            df.loc[:, f'max_pre{span}_price'] = df.groupby(level='ts_code').high.apply(lambda x: x.rolling(window=span).max().shift(1))
            df.loc[:, f'min_pre{span}_price'] = df.groupby(level='ts_code').low.apply(lambda x: x.rolling(window=span).min().shift(1))
        logger.debug(f'df Memory: {df.memory_usage(deep=True)}')

        gc.collect()

        # cache it
        expire_date = pdl.today().add(days=expire_days).to_date_string()
        df_file_path = f'{ROOT_PATH}/tmp/priceslim_{start_date}_{end_date}_{expire_date}.feather'

        df.reset_index(inplace=True)
        df.to_feather(df_file_path)
        df.set_index(['ts_code', 'trade_date'], inplace=True)

    return df



import re
import time
from pprint import pprint

class Tonghuashun:
    # 同花顺自选股列表相关
    url = {'get': 'http://pop.10jqka.com.cn/getselfstockinfo.php',
           'modify': 'http://stock.10jqka.com.cn/self.php'}

    def __init__(self, uid, cookie):
        self.uid = uid
        self.cookie = cookie  # 该用户登录的cookie
        self.headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:56.0) '
                'Gecko/20100101 Firefox/56.0',
                'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
                'Accept-Encoding': 'gzip, deflate',
                'Referer': 'http://stock.10jqka.com.cn/my/zixuan.shtml',
                'DNT': '1'
            }
        self.cookie = cookie
        self.stocks = pd.DataFrame()  # 同花顺自选股清单

    def get_stocks(self):
        # 获取同花顺自选股列表
        try:
            payload = {'callback': 'callback' + str(int(time.time() * 1000))}
            response = r.get(Tonghuashun.url['get'], params=payload, headers=self.headers, cookies=self.cookie, timeout=10)
            # pprint(response.content)
            self.stocks = pd.DataFrame(response.json())
        except Exception as e:
            logger.info(''.join(['get_stocks @', self.uid, '; error:', e]))
            pprint(payload)
            pprint(self.headers)
            return False
        else:
            # pprint(self.stocks)
            return self.stocks

    def modify_stock(self, code, method, pos='1'):
        # 更改同花顺自选股列表
        # method: add 添加, del 删除, exc 排序
        # pos: 排序用的序号, 从1开始
        try:
            payload = {'add': {'stockcode': code, 'op': 'add'},
                       'del': {'stockcode': code, 'op': 'del'},
                       'exc': {'stockcode': code, 'op': 'exc', 'pos': pos, 'callback': 'callbacknew'}
                       }
            # self.get_stocks()
            response = r.get(Tonghuashun.url['modify'], params=payload[method], headers=self.headers, timeout=10)
            # pprint(response.content)
            response = response.content.decode('gbk')
            logger.info(''.join(['modify_stocks', method, pos, code, response]))
            if response == u'修改自选股成功':
                response = True
        except Exception as e:
            logger.info(''.join(['modify_stock', method, code, '@', self.uid, '; error:', e]))
            pprint(payload[method])
            pprint(self.headers)
            return False
        else:
            self.get_stocks()
            return response



def df_to_text(df, prefix_newline=False):
    txt = []
    if prefix_newline:
        txt.append("")
    for raw in df.iterrows():
        row = raw[1]
        if 'conseq_up_num' in df.columns:
            upstop_str = f'{int(row.conseq_up_num)}板 ({row.up_type}), '
        else:
            upstop_str = ''
        txt.append(f'[{row.name}][{row["name"]}] {merge_text_list(row.plate_name)}, {upstop_str}流值{round(row.circ_mv/10000, 2)}亿，量比{round(row.vol_ratio,2)}，{round(row.amount/100000, 2)}亿，trf{round(row.turnover_rate_f,0)}%')
    return "\n".join(txt)


def send_notification(text):
    wechat_bot.send_text(text)

def send_stra_result(res, strategy=None):
    if len(res) > 0:
        styled_res = style_full_df(res)
        logger.info(res.name)
        out_image = f'./output/{strategy}_{next_date}.png'
        dfi.export(styled_res, out_image)
        wechat_bot.send_text(df_to_text(res))
        wechat_bot.send_image(out_image)
        logger.info(f'{len(res)} results has been sent.')
        logger.debug(res)
    else:
        wechat_bot.send_text(f'Strategy {strategy} got nothing on {end_date}.')


if __name__ == '__main__':
    options = bot_options()

    if options.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    logger.debug("It's a good show.")

    from utils.datetimes import biquater_ago_date, quater_ago_date, end_date as tdu_end_date
    start_date = options.start_date if options.start_date else quater_ago_date
    end_date = options.end_date if options.end_date else tdu_end_date

    # with open('./tmp/debug.json', 'r') as f:
        # txt = f.readlines()
    # json_content = json.loads(txt[0])
    # cookies = {}
    # for cookie in json_content:
        # cookies[cookie['name']] = cookie['value']

    # logger.debug(cookies)

    # ths = Tonghuashun(uid='mx_516201474', cookie=cookies)
    # stks = ths.get_stocks()
    # logger.info(stks)
    # exit()

    # if (today_date != end_date) and (today_date not in tdu.future_trade_days(start_date=end_date)):
    if False:
        # not trading day
        wechat_bot.send_text(f'[{today_date}] Today is not a trading day. Have fun!')
    else:
        task = Bot(start_date=start_date, end_date=end_date, task_name=options.task_name)
        task.perform()
