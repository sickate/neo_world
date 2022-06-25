# Bot 
# Author: tuo

from env import *
from sqlbase import *

from models import *
from utils.datasource import *
from utils.datetimes import *
from utils.psql_client import *
from utils.stock_filter import *
from utils.stock_utils import *
from utils.strategy import *
# from utils.notifiers import *
from utils.logger import *
from data_tasks import *


def refine_variables(df, stra):
    rules = stra.rules
    for k, v in rules.items():
        if k not in df:
            logger.warn(f'Var {k} is not there, generating...')


if __name__ == '__main__':

    start_date = '2021-11-24'
    end_date = '2022-06-23'

    expire_date = pdl.today().add(days=5).to_date_string()
    df_file_path = f'{ROOT_PATH}/tmp/price_{start_date}_{end_date}_{expire_date}.feather'

    # origin
    # dc, df = init_data(biquater_ago_date, end_date, expire_days=5)
    # logger.info(len(df))

    # new
    dc = DataCenter(start_date, end_date)
    stk_basic = dc.get_stock_basics()

    upstop = dc.get_upstops()
    mf = dc.get_money_flow()
    mf.loc[:, 'dde_amt'] = (mf.buy_elg_amount + mf.buy_lg_amount - mf.sell_elg_amount - mf.sell_lg_amount) * 10 # unit从万变成千
    mf.loc[:, 'dde_vol'] = (mf.buy_elg_vol + mf.buy_lg_vol - mf.sell_elg_vol - mf.sell_lg_vol) / 10 # unit从手换成千股
    mf.loc[:, 'dde'] = round(mf.dde_vol / mf.float_share * 10, 2) # 千股除以万股，/10,再换成 pct，*100 =》 *10
    mf = mf[['dde_amt', 'dde_vol', 'dde']]

    logger.info(f'Start processing price df...')
    price = load_stock_prices(start_date=start_date, end_date=end_date, fast_load=True)

    logger.info(f'Start processing adj price...')
    # price = dc.get_price()
    price = gen_adj_price(price, replace=True)

    logger.info(f'Join price with other columns...')
    df = price.join(mf).join(upstop.drop(columns=['pct_chg', 'close'])).join(stk_basic[['name', 'list_date']])
    df = df[~df.list_date.isna()] # remove already 退市的

    logger.info(f'Start processing complex price...')
    df.loc[:, 'pre_close_6'] = df.groupby(level='ts_code').close.shift(6)
    df.loc[:, 'pre_close_21'] = df.groupby(level='ts_code').close.shift(21)
    df.loc[:, 'pre_pct_chg'] = df.groupby(level='ts_code').pct_chg.shift(1)
    df.loc[:, 'pre5_pct_chg'] = (df.pre_close/df.pre_close_6 - 1) * 100
    df.loc[:, 'pre20_pct_chg'] = (df.pre_close/df.pre_close_21 - 1) * 100

    # 涨停类型
    # 当日是否涨停
    logger.info(f'Calculating upstop variables...')
    df.loc[:, 'upstop_num'] = df.limit.apply(lambda lim: 1 if lim == 'U' else -1 if lim =='D' else 0)
    # 累计连续涨停个数
    tmp = df.groupby(level='ts_code').upstop_num.cumsum()
    tmp2 = tmp.mask(df.upstop_num == 1).groupby(level='ts_code').ffill()
    df.loc[:, 'conseq_up_num'] = tmp.sub(tmp2, fill_value=0).astype(int)
    df.loc[:, 'pre_conseq_up_num'] = df.groupby('ts_code').conseq_up_num.shift(1)

    # 前日开板次数
    df.loc[:, 'pre_open_times'] = df.groupby('ts_code').open_times.shift(1)
    df.loc[:, 'pre5_upstops'] = df.groupby(level='ts_code').upstop_num.apply(lambda x: x.rolling(window=5, min_periods=1).sum())
    df.loc[:, 'pre10_upstops'] = df.groupby(level='ts_code').upstop_num.apply(lambda x: x.rolling(window=10, min_periods=1).sum())
    df.loc[:, 'upstop_price'] = df.pre_close.apply(up_stop_price)
    df.loc[:, 'next_limit']   = df.groupby(level='ts_code').limit.shift(-1)

    logger.info('Calculating MAs...')
    df = gen_ma(df, col='close', mavgs=[5, 10, 30])

    # 计算 list_days
    # logger.info('Calculating list_days...')
    # df['cur_date'] = df.index.get_level_values('trade_date').map(lambda x: x.strftime('%Y-%m-%d'))
    # tmp = df.loc[df.groupby('ts_code').head(1).index]
    # tmp['list_days'] = tmp.cur_date.map(lambda x: self.trade_days.index(x)) - \
                       # tmp.list_date.map(lambda x: self.trade_days.index(x)) + 1
    # df_init['list_days'] = tmp['list_days']
    # df_init.list_days.fillna(1, inplace=True)
    # df_init.list_days = df_init.groupby('ts_code').list_days.cumsum()

    # 计算 bar type
    logger.info('Calculating bar_type...')
    df.loc[:, 'bar_type'] = df.apply(f_calc_yinyang, axis=1)
    df.loc[:, 'pre_bar_type'] = df.groupby('ts_code').bar_type.shift(1)
    df.loc[:, 'pre2_bar_type'] = df.groupby('ts_code').bar_type.shift(2)

    auctions = load_table(Auction, start_date, end_date).sort_index()
    df = df.join(self.auctions[['auc_vol', 'auc_amt']])

    logger.info('Performing shift to get prev signals...')
    # df.loc[:, 'cvo'] = df.pct_chg - df.open_pct
    # for ind in ['open_times', 'fl_ratio', 'fc_ratio', 'strth', 'amount', 'amp', 'vol', 'vol_ratio']:
        # df.loc[:, f'pre_{ind}'] = df.groupby(level='ts_code')[ind].shift(1)
    # for ind in ['cvo', 'auc_amt', 'auc_vol', 'bar_type', 'vol_ratio']:
    #     df.loc[:, f'next_{ind}'] = df.groupby(level='ts_code')[ind].shift(-1)
    # df_init.loc[:, 'next_auc_pvol_ratio'] = df_init.next_auc_amt/df_init.amount

    # df_init.loc[:, 'dde_amt'] = (df_init.buy_elg_amount + df_init.buy_lg_amount - df_init.sell_elg_amount - df_init.sell_lg_amount) * 10 # unit从万变成千
    # df_init.loc[:, 'dde_vol'] = (df_init.buy_elg_vol + df_init.buy_lg_vol - df_init.sell_elg_vol - df_init.sell_lg_vol) / 10 # unit从手换成千股
    # df_init.loc[:, 'dde'] = round(df_init.dde_vol / df_init.float_share * 10, 2) # 千股除以万股，/10,再换成 pct，*100 =》 *10

    # cache it
    logger.info('Saving cache file...')
    df.reset_index().to_feather(df_file_path)
 



    logger.info(up_stra.rules)
    refine_variables(dc.get_price(), up_stra)



