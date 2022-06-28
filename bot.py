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
    'circ_mv', 'total_mv', 'turnover_rate_f', 'vol_ratio', 'amount', 'dde', 'dde_amt',
]
review_cols = slim_cols + ['next_auc_amt', 'next_auc_pvol_ratio', 'next_open_pct', 'next_pct_chg']

auc_preview_cols = preview_cols + ['next_auc_amt', 'next_auc_pvol_ratio', 'next_open_pct']

notification_cols = [
    'name', 'plate_name', 'close', 'conseq_up_num',
    'circ_mv', 'turnover_rate_f', 'vol_ratio', 'amount'
]

def refine_variables(df, stra):
    rules = stra.rules
    for k, v in rules.items():
        if k not in df:
            logger.warn(f'Var {k} is not there, generating...')


class Bot():

    def __init__(self, start_date, end_date, task_name, verbose=False):
        if verbose:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)
        logger.debug("It's a good show.")

        logger.info(f'Start working on [{start_date} - {end_date}] Task: {task_name}... ')
        self.end_date = end_date

        # prepare stock data
        self.df = slim_init(start_date, end_date, 10)
        clean_cache_files()

        # top cons
        plate_mapping = ak_all_plates(use_cache=True)
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
        pass


    def before_mkt(self):
        box_stra.rules['conseq_up_num'] = [{'op': '=', 'val': 1}] # remove list_days stra
        logger.debug(box_stra.rules)
        res, keys = box_stra.get_result(self.df, self.end_date)
        logger.info(f'Found {len(res)}')
        res.sort_values('turnover_rate_f', inplace=True)
        res = res.join(self.top_cons)
        send_notification(res[preview_cols], strategy='BOX')


    def open_mkt(self):
        up_auc_vol_stra = Strategy(name='up & auc', stock_filter=StockFilter(end_date).not_st().tui(anti=True).zb())
        up_stra.rules['conseq_up_num'] = [{'op': '>=', 'val': 1}, {'op': '<=', 'val': 3}] # remove list_days stra
        up_auc_vol_stra.merge_other(up_stra)
        df2, a = up_auc_vol_stra.get_result(df=self.df, trade_date=end_date)
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
                send_notification(df4[auc_preview_cols], strategy='AUC')
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
        zha_df, a = stra_zha.get_result(df=self.df, trade_date=end_date)
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
                send_notification(zha_df_res[slim_cols], strategy='Zha')
                break


def slim_init(start_date, end_date, expire_days=30):

    search_pattern = glob.glob(f'{ROOT_PATH}/tmp/price_{start_date}_{end_date}_*_slim.feather')
    for f in search_pattern:
        # read cache
        logger.info(f'Found cache file: {f}, loading...')
        df = pd.read_feather(f).set_index(['ts_code', 'trade_date'])
        break
    else:
        logger.info(f'No cache. Start processing...')
        dc = DataCenter(start_date, end_date)

        stk_basic = dc.get_stock_basics()
        logger.debug(f'stk_basic Memory: {stk_basic.memory_usage(deep=True)}')

        upstop = dc.get_upstops()
        logger.debug(f'upstop Memory: {upstop.memory_usage(deep=True)}')
        mf = dc.get_money_flow()
        mf.loc[:, 'dde_amt'] = (mf.buy_elg_amount + mf.buy_lg_amount - mf.sell_elg_amount - mf.sell_lg_amount) * 10 # unit从万变成千
        mf.loc[:, 'dde_vol'] = (mf.buy_elg_vol + mf.buy_lg_vol - mf.sell_elg_vol - mf.sell_lg_vol) / 10 # unit从手换成千股
        mf = mf[['dde_amt', 'dde_vol']]
        logger.debug(f'mf Memory: {mf.memory_usage(deep=True)}')

        logger.info('Processing auction data...')
        auctions = load_table(Auction, start_date, end_date)[['auc_vol', 'auc_amt']]
        logger.debug(f'auction Memory: {auctions.memory_usage(deep=True)}')

        logger.info(f'Start processing price df...')
        price = load_stock_prices(start_date=start_date, end_date=end_date, fast_load=True)
        logger.debug(f'price Memory: {price.memory_usage(deep=True)}')

        logger.info(f'Start processing adj price...')
        # price = dc.get_price()
        price = gen_adj_price(price, replace=True)
        logger.debug(f'price Memory after adj: {price.memory_usage(deep=True)}')

        logger.info(f'Join price with other columns...')
        df = (
            price.join(mf)
                 .join(upstop.drop(columns=['pct_chg', 'close', 'fc_ratio', 'fl_ratio', 'fd_amount', 'last_time']))
                 .join(stk_basic[['name', 'list_date']].astype(dtype="string[pyarrow]"))
                 .join(auctions)
                 .drop(columns=[
                    'change', 'adj_factor', 'pe', 'pe_ttm', 'total_share',
                    'free_share', 'last_factor', 'norm_adj', 'adj_vol', 'amp',
                  ])
        )
        logger.debug(f'{len(df)} df Memory after join: {df.memory_usage(deep=True)}')

        # del mf
        # del upstop
        # del stk_basic
        # del auctions
        # gc.collect()

        df = StockFilter(end_date).tui(anti=True).not_st().filter(df)

        logger.debug(f'{len(df)} df Memory after join: {df.memory_usage(deep=True)}')

        # df = df[~df.list_date.isna()] # remove already 退市的
        df.drop(columns=['list_date'], inplace=True)

        df.loc[:, 'dde'] = round(df.dde_vol / df.float_share * 10, 2) # 千股除以万股，/10,再换成 pct，*100 =》 *10
        logger.debug(f'df Memory: {df.memory_usage(deep=True)}')

        logger.info(f'Start processing complex price...')
        df.loc[:, 'pre_close_6'] = df.groupby(level='ts_code').close.shift(6)
        df.loc[:, 'pre_close_21'] = df.groupby(level='ts_code').close.shift(21)
        df.loc[:, 'pre_pct_chg'] = df.groupby(level='ts_code').pct_chg.shift(1)
        df.loc[:, 'pre5_pct_chg'] = (df.pre_close/df.pre_close_6 - 1) * 100
        df.loc[:, 'pre20_pct_chg'] = (df.pre_close/df.pre_close_21 - 1) * 100
        logger.debug(f'df Memory: {df.memory_usage(deep=True)}')

        # 当日是否涨停
        logger.info(f'Calculating upstop variables...')
        df.loc[:, 'upstop_num'] = df.limit.apply(lambda lim: 1 if lim == 'U' else -1 if lim =='D' else 0).astype(dtype='int8')
        df.loc[:, 'limit'] = df['limit'].astype(dtype="string[pyarrow]")

        # 累计连续涨停个数
        tmp = df.groupby(level='ts_code').upstop_num.cumsum()
        tmp2 = tmp.mask(df.upstop_num == 1).groupby(level='ts_code').ffill()
        df.loc[:, 'conseq_up_num'] = tmp.sub(tmp2, fill_value=0).astype('int8')
        df.loc[:, 'pre_conseq_up_num'] = df.groupby('ts_code').conseq_up_num.shift(1)

        logger.info(f'Calculating previous upstop variables...')
        df.loc[:, 'pre_open_times'] = df.groupby('ts_code').open_times.shift(1)
        df.loc[:, 'pre5_upstops'] = df.groupby(level='ts_code').upstop_num.apply(lambda x: x.rolling(window=5, min_periods=1).sum()).astype('int8')
        df.loc[:, 'pre10_upstops'] = df.groupby(level='ts_code').upstop_num.apply(lambda x: x.rolling(window=10, min_periods=1).sum()).astype('int8')
        df.loc[:, 'upstop_price'] = df.pre_close.apply(up_stop_price)
        df.loc[:, 'next_limit']   = df.groupby(level='ts_code').limit.shift(-1)
        logger.debug(f'df Memory: {df.memory_usage(deep=True)}')

        logger.info('Calculating MAs...')
        df = gen_ma(df, col='close', mavgs=[5, 10, 30, 60])
        df.rename(columns={'volume_ratio':'vol_ratio'}, inplace=True)
        df = gen_ma(df, mavgs=[5], col='vol', add_shift=1)
        # df.loc[:, 'vol_ratio'] = df.vol / df.ma_vol_5
        logger.debug(f'df Memory: {df.memory_usage(deep=True)}')
 
        # gc.collect()

        # 计算 bar type
        # logger.info('Calculating bar_type...')
        # df.loc[:, 'bar_type'] = df.apply(f_calc_yinyang, axis=1).astype('string[pyarrow]')
        # df.loc[:, 'pre_bar_type'] = df.groupby('ts_code').bar_type.shift(1)
        # logger.debug(f'df Memory: {df.memory_usage(deep=True)}')

        logger.info('Calculating up_type...')
        df.loc[:, 'up_type'] = df[df.limit=='U'].apply(f_set_upstop_types, axis=1).astype('string[pyarrow]')
        logger.debug(f'df Memory: {df.memory_usage(deep=True)}')

        logger.info('Performing shift to get prev signals...')
        # df.loc[:, 'cvo'] = df.pct_chg - df.open_pct
        for ind in ['up_type', 'vol', 'vol_ratio']:
            df.loc[:, f'pre_{ind}'] = df.groupby(level='ts_code')[ind].shift(1)
        logger.debug(f'df Memory: {df.memory_usage(deep=True)}')

        # Max in prev days
        for span in [10, 120]:
            df.loc[:, f'max_pre{span}_price'] = df.groupby(level='ts_code').high.apply(lambda x: x.rolling(window=span).max().shift(1))
            df.loc[:, f'min_pre{span}_price'] = df.groupby(level='ts_code').low.apply(lambda x: x.rolling(window=span).min().shift(1))
        logger.debug(f'df Memory: {df.memory_usage(deep=True)}')

        gc.collect()

        # cache it
        expire_date = pdl.today().add(days=expire_days).to_date_string()
        df_file_path = f'{ROOT_PATH}/tmp/price_{start_date}_{end_date}_{expire_date}_slim.feather'

        df.reset_index(inplace=True)
        df.to_feather(df_file_path)
        df.set_index(['ts_code', 'trade_date'], inplace=True)

    return df


def df_to_text(df, prefix_newline=False):
    txt = []
    if prefix_newline:
        txt.append("")
    for raw in df.iterrows():
        row = raw[1]
        txt.append(f'[{row.name}][{row["name"]}] {merge_text_list(row.plate_name)}, {int(row.conseq_up_num)}板 ({row.up_type})，流值{round(row.circ_mv/10000, 2)}亿，量比{round(row.vol_ratio,2)}，{round(row.amount/100000, 2)}亿，trf{round(row.turnover_rate_f,0)}%')
    return "\n".join(txt)


def send_notification(res, strategy):
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
    from utils.datetimes import biquater_ago_date, quater_ago_date, end_date as tdu_end_date

    options = bot_options()
    start_date = options.start_date if options.start_date else biquater_ago_date
    end_date = options.end_date if options.end_date else tdu_end_date

    if (today_date != end_date) and (today_date not in tdu.future_trade_days(start_date=end_date)):
        # not trading day
        pass
    else:
        task = Bot(start_date=start_date, end_date=end_date, task_name=options.task_name, verbose=options.verbose)
        task.perform()
