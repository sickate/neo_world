import sys
sys.path.append('./')

from env import np, trange, tqdm
from IPython.core.display import HTML

# import numpy as np
# from tqdm.auto import

from sqlbase import db_session, engine
from models import *

from utils.psql_client import read_sql, read_pg, load_table
from utils.pd_styler import *
from utils.plot_plotly import plot_k_plotly
from utils.calculators import *
from utils.indicators import *
from utils.datetimes import trade_day_util as tdu


BASIC_COLS = ['name', 'open', 'high', 'low', 'close', 'pct_chg', 'amount']
ORI_COLS = ['first_time', 'last_time', 'open_times', 'fc_ratio', 'fl_ratio', 'limit']
ADDED_COLS = ['upstop_num', 'conseq_up_num', 'post_up_num', 'up_type']
Y_COLS = ['pct_chg', 'close_open_pct', 'next_high_open_pct', 'next_pct_chg', 'next2_pct_chg', 'next3_pct_chg', 'next10_pct_chg', 'next20_pct_chg']

OREDER_COLS = ['name', 'op_num', 'vol', 'price', 'expense', 'close', 'total_vol', 'total_expense', 'hold_value', 'profit', 'reason']

# 获得上涨或下跌最高的 top n ts_code
def top_diff(df, start_date=None, end_date=None, top_n=20, price_col='close', date_col='trade_date'):
    if not start_date and (not end_date):
        tmpdf = df
    else:
        tmpdf = df[(df[date_col] >= start_date) & (df[date_col] <= end_date)]
    start_series = tmpdf.sort_values(by=['ts_code', date_col]) \
                        .groupby('ts_code').first()[[date_col, price_col]] \
                        .rename(columns={
                            date_col: 'start_date',
                            price_col: 'start_price'})
    end_series = tmpdf.sort_values(by=['ts_code', date_col]) \
                        .groupby('ts_code').last()[[date_col, price_col]] \
                        .rename(columns={
                            date_col: 'end_date',
                            price_col: 'end_price'})
    #result_series = pd.merge(start_series, end_series)
    result_series = start_series.join(end_series)
    result_series['diff'] = (result_series['end_price'] - result_series['start_price'])/result_series['start_price']
    return result_series.sort_values('diff')


#####################################################################
# Variable Generators
#####################################################################

def gen_price_data(df):
    # Calc price related
    print('Processing price variables...')

    # To test
    # pre_close_1 == pre_close
    df.loc[:, 'pre_close_4'] = df.groupby(level='ts_code').close.shift(4)
    df.loc[:, 'pre_close_6'] = df.groupby(level='ts_code').close.shift(6)
    df.loc[:, 'pre_close_11'] = df.groupby(level='ts_code').close.shift(11)
    df.loc[:, 'pre_close_21'] = df.groupby(level='ts_code').close.shift(21)

    # pre3_pct_chg:  截止昨天的，过去 3 天的累计涨幅 => (t-1收盘价 - t-4收盘价)/t-4收盘价
    df.loc[:, 'pre_pct_chg'] = df.groupby(level='ts_code').pct_chg.shift(1) # pre_pct_chg = pre_pct_chg_1 (昨天一天的涨幅)
    df.loc[:, 'pre3_pct_chg'] = (df.pre_close/df.pre_close_4 - 1) * 100
    df.loc[:, 'pre5_pct_chg'] = (df.pre_close/df.pre_close_6 - 1) * 100
    df.loc[:, 'pre10_pct_chg'] = (df.pre_close/df.pre_close_11 - 1) * 100
    df.loc[:, 'pre20_pct_chg'] = (df.pre_close/df.pre_close_21 - 1) * 100

    df.loc[:, 'next_close_1'] = df.groupby(level='ts_code').close.shift(-1)
    df.loc[:, 'next_close_2'] = df.groupby(level='ts_code').close.shift(-2)
    df.loc[:, 'next_close_3'] = df.groupby(level='ts_code').close.shift(-3)
    df.loc[:, 'next_close_10'] = df.groupby(level='ts_code').close.shift(-10)
    df.loc[:, 'next_close_20'] = df.groupby(level='ts_code').close.shift(-20)
    # 未来 x 日累计涨幅（相对当日收盘价)
    df.loc[:, 'next_pct_chg'] =  (df.next_close_1/df.close-1) * 100
    df.loc[:, 'next2_pct_chg'] = (df.next_close_2/df.close-1) * 100
    df.loc[:, 'next3_pct_chg'] = (df.next_close_3/df.close-1) * 100
    df.loc[:, 'next10_pct_chg'] = (df.next_close_10/df.close-1) * 100
    df.loc[:, 'next20_pct_chg'] = (df.next_close_20/df.close-1) * 100

    # Max in prev days
    for span in [10, 20, 30, 60, 120]:
        df.loc[:, f'max_pre{span}_price'] = df.groupby(level='ts_code').high.apply(lambda x: x.rolling(window=span).max().shift(1))
        df.loc[:, f'min_pre{span}_price'] = df.groupby(level='ts_code').low.apply(lambda x: x.rolling(window=span).min().shift(1))

    df.loc[:, 'avg_price'] = round(df.amount * 10 / df.vol, 2)
    return df


def calc_vol_types(df, mavgs=None):
    '''
        Generate ma_vols and vol_ratio
    '''
    if mavgs is None:
        mavgs = [5,10,20]
    df = gen_ma(df, mavgs=mavgs, col='vol', add_shift=1)
    df.loc[:, 'vol_ratio'] = df.vol / df.ma_vol_5
    df.loc[:, 'vol_ratio_long'] = df.vol / df.ma_vol_20
    df.loc[:, 'vol_type'] = df.apply(f_set_vol_types, axis=1)
    df.loc[:, 'pre_vol_type'] = df.groupby('ts_code').vol_type.shift(1)
    df.loc[:, 'pre_trf'] = df.groupby(level='ts_code').turnover_rate_f.shift(1)
    return df


#####################################################################
# Visualized Anayltics Helpers
#####################################################################

def compare(dfs, cols=Y_COLS):
    summary = """
    Compare n dataframes.
    """
    for col in cols:
        for ind in range(len(dfs)):
            df = dfs[ind]
            df = df.replace([np.inf, -np.inf], np.nan)
            print(f'DF #{ind} {col} mean: {round(df[col].dropna().mean(), 4)}, max: {round(df[col].dropna().max(), 4)}, min: {round(df[col].dropna().min(), 4)}')
        print('-------------')


# 展示一支股票前后一段时期的指标
def show_span(df, code, t_date, pre_span=10, post_span=5, add_cols=None, plot=False):
    if len(code) == 6:
        code = add_postfix(code, type='ts')
    s_date = tdu.past_trade_days(end_date=t_date)[-pre_span]
    e_date = tdu.future_trade_days(start_date=t_date)[post_span]
    cols = ['up_type', 'pct_chg', 'auc_ratio_all', 'auc_vol_ratio', 'auc_vol', 'turnover_rate_f', 'net_pct_main', 'net_trf_main', 'net_mf_vol', 'vol_ratio', 'vol']
    if add_cols is not None and len(add_cols) > 0:
        cols = cols + add_cols

    tmp = df.loc[code].loc[s_date:e_date]
    print(f'名称：{tmp.at[e_date, "name"]}, 板块：{tmp.at[e_date, "ind_name"]}, 流值：{round(tmp.at[e_date, "circ_mv"]/10000, 2)}亿, 市值：{round(tmp.at[e_date, "total_mv"]/10000, 2)}亿')

    if plot:
        quick_k(df, code)

    return style_df(tmp[cols])


# 快速 plot 一支股票的 k 线图
def quick_k(df, code, span=70):
    if len(code) == 6:
        code = add_postfix(code, type='ts')
    tmp = df.loc[code]
    if not contains(['ma_close_5', 'ma_close_10', 'ma_close_30'], df.columns):
        tmp = gen_ma(tmp, col='close', mavgs=[5, 10, 30], single_index=True)
    tmp = tmp.tail(span)
    # tmp.loc[:,'hovertext'] = tmp.apply(f_get_hovertext, axis=1)
    plot_k_plotly(tmp, verbose=False)


# 计算过去 n 天的总体情绪指标
def upstop_trend(df, end_date, n_days=10):
    trade_days = tdu.past_trade_days(end_date)[-n_days:]
    past_upstops = {}
    upstop_trend_df = pd.DataFrame()

    for i, tdate in enumerate(trade_days):
        t_df = df.xs(tdate, level='trade_date', drop_level=True)
        p_up = t_df[t_df.pre_conseq_up_num > 0]
        p_ups = t_df[t_df.pre_conseq_up_num > 1]
        p_up_t_up = p_up[p_up.limit=='U']
        p_up_t_noup = p_up[p_up.limit!='U']
        t_up = t_df[t_df.limit=='U']
        t_ups = t_df[t_df.conseq_up_num > 1]

        # 昨日跌幅大的票
        p_big_dn = t_df[t_df.pre_pct_chg <= -7]

        mapping = {
            'upst_cnt': '涨停数',
            'cons_upst_cnt': '连板数',
            'pre_up_pct': '昨涨停平均涨幅',
            'pre_ups_pct': '昨连板平均涨幅',
            'pre_up_cons_pct': '昨涨停晋级率',
            'p_up_t_noup_pct': '掉队股平均涨幅'
        }

        upstop_trend_df.loc[tdate, 'upst_cnt'] = len(t_up)
        upstop_trend_df.loc[tdate, 'cons_upst_cnt'] = len(t_ups)
        if len(p_up) == 0:
            print(tdate)
            upstop_trend_df.at[tdate, 'pre_up_cons_pct'] = 0
        else:
            pre_up_cons_pct = len(p_up_t_up)/len(p_up) * 100
            upstop_trend_df.at[tdate, 'pre_up_cons_pct'] = pre_up_cons_pct
        upstop_trend_df.loc[tdate, 'pre_up_pct'] = p_up.pct_chg.mean()
        upstop_trend_df.loc[tdate, 'pre_ups_pct'] = p_ups.pct_chg.mean()
        upstop_trend_df.loc[tdate, 'p_up_t_noup_pct'] = p_up_t_noup.pct_chg.mean()
        upstop_trend_df.loc[tdate, 'p_big_dn_pct'] = p_big_dn.pct_chg.mean()

    return upstop_trend_df


def add_postfix(ts_code, type='ts'):
    if type == 'ts':
        if ts_code.startswith('3') or ts_code.startswith('0'):
            return ts_code + '.SZ'
        elif ts_code.startswith('6'):
            return ts_code + '.SH'
        else:
            return ts_code + '.BJ'
    elif type == 'ak':
        if ts_code.startswith('3') or ts_code.startswith('0'):
            return 'sz' + ts_code.split('.')[0]
        elif ts_code.startswith('6'):
            return 'sh' + ts_code.split('.')[0]
        else:
            return 'bj' + ts_code.split('.')[0]
    elif type == 'jq':
        return jq.normalize_code([ts_code])[0]
    else:
        return None


def get_name_from_ts_code(ts_code):
    q = (
        db_session.query(StockBasic)
            .filter(StockBasic.ts_code == ts_code)
    )
    # df = pd.read_sql(q.statement, engine)
    df = read_sql(q.statement)
    return df[df.ts_code == ts_code].at[0, 'name']


def get_ts_code_from_name(name):
    q = (
        db_session.query(StockBasic)
            .filter(StockBasic.name == name)
    )
    # df = pd.read_sql(q.statement, engine)
    df = read_sql(q.statement)
    return df[df.name == name].at[0,'ts_code']


#####################################################################
# Log Utils (TODO)
#####################################################################

def cstr(s, color='black'):
    return "<text style=color:{}>{}</text>".format(color, s)


def bstr(s):
    return "<b>{}</b>".format(s)


def write_log(ts_code, order_at, vol, price, reason=None):
    if len(ts_code) == 6:
        ts_code = add_postfix(ts_code)
    elif len(ts_code) < 6:
        ts_code = get_ts_code_from_name(ts_code)
    name = get_name_from_ts_code(ts_code)
    action_str = cstr('买入', 'red') if vol > 0 else cstr('卖出', 'green')
    if ':' not in order_at and len(order_at)==17:
        order_at = order_at[0:13]+':'+order_at[13:15]+':'+order_at[15:17]
    order = Order(ts_code, order_at)
    order.write_log(vol, price)
    order.reason = reason

    try:
        db_session.add(order)
        db_session.commit()
    except Exception as e:
        print('Saving log error')
        db_session.rollback()
    finally:
        return order, HTML(f'[{order.id}] [{order_at}] {action_str} {ts_code} {bstr(name)} {abs(vol)} 股, 均价 {price} 元, 总金额 {cn_round_price(abs(vol * price))} 元, 税费 {cn_round_price(order.fee + order.tax)} 元.')


def trade_summary(start_date, end_date, realtime=False):
    orders = load_table(Order, start_date, end_date)
    orders['amount'] = -orders.price * orders.vol

    stk_basic = read_pg(table='stock_basic').set_index('ts_code')
    orders = orders.join(stk_basic[['name']])
    orders = orders.reset_index().set_index('ts_code')
    if realtime:
        price_col = 'current'
        price = pd.DataFrame()
        pass
    else:
        price_col = 'close'
        price = load_table(Price, end_date, end_date)
        orders = orders.join(price[[price_col]].reset_index().set_index('ts_code').drop(columns='trade_date'))

    sum_df = orders.reset_index().groupby('ts_code').agg({'trade_date': ['min','max'], 'vol':'sum', 'amount':'sum', price_col:'first', 'name':'first', 'reason':'first', 'fee':'sum', 'tax': 'sum'})
    sum_df.columns = ['_'.join(col) for col in sum_df.columns]
    sum_df.rename(columns={'name_first': 'name', f'{price_col}_first': price_col, 'reason_first': 'reason', 'tax_sum':'tax', 'fee_sum':'fee'}, inplace=True)
    sum_df['hold_value'] = sum_df[price_col] * sum_df.vol_sum
    sum_df['current_profit'] = sum_df.hold_value + sum_df.amount_sum - sum_df.fee - sum_df.tax
    sum_df['current_pct_chg'] = sum_df.current_profit / (sum_df.hold_value - sum_df.current_profit) * 100
    cols = sum_df.columns.to_list()
    dumped = sum_df[sum_df.hold_value==0].sort_values('trade_date_min')[['name', 'trade_date_min', 'trade_date_max', 'vol_sum', 'hold_value', 'current_profit', 'close', 'reason']]
    holding = sum_df[sum_df.hold_value>0].sort_values('trade_date_min')[['name', 'trade_date_min', 'trade_date_max', 'vol_sum', 'amount_sum', 'hold_value', 'current_profit', 'current_pct_chg', 'close', 'reason']]

    print(f'From {start_date} to {end_date} summary:')
    print(f'Total {len(dumped)} stocks. Winning rate: {to_pct(len(dumped[dumped.current_profit>0])/len(dumped))}%')
    print(f'Total profit: {cn_round_price(dumped.current_profit.sum())}, Max profit: {dumped.current_profit.max()}, Max lost: {dumped.current_profit.min()}.')
    print(f'Current holding: {len(holding)} stocks. Current profit: {holding.current_profit.sum()}.')

    return holding, dumped, orders


def trade_summary2(start_date, end_date, price):
    orders = load_table(model=Order, start_date=start_date, end_date=end_date)
    orders = orders.join(price[['name', 'close']])

    # 根据当前持仓总量拆分不同的开仓记录
    orders.loc[:,'total_vol'] = orders.groupby('ts_code').vol.cumsum()
    orders.loc[:,'op_num'] = orders.apply(lambda r: 1 if r['total_vol'] == r['vol'] else 0, axis=1)
    orders.loc[:,'op_num'] = orders.groupby('ts_code').op_num.cumsum()

    orders.loc[:,'hold_value'] = orders.total_vol * orders.close # 当日的实时价值
    orders.loc[:,'expense'] = -(orders.price * orders.vol + orders.tax + orders.fee) # 当笔交易的支出（卖出操作为收入）
    orders.loc[:, 'total_expense'] = orders.groupby(['ts_code', 'op_num']).expense.cumsum() # 当前开仓的总净支出
    # 截止当日，当前开仓的总盈利
    orders.loc[:, 'profit'] = orders.apply(lambda r: r['hold_value'] + r['total_expense'] if r['total_vol'] != r['vol'] else r['expense']+r['close']*r['vol'], axis=1)
    orders.sort_index(inplace=True)

    # 已清仓交易
    # TODO: 开仓时间，清仓时间列计算
    finished = orders[orders.total_vol==0]

    # buy reason
    # orders.reset_index(), on=['trade_date', 'op_num']

    # finished.loc[:, 'buy_reason'] = finished.reset_index().join(orders.reset_index(), on=['trade_date', 'op_num'])
    # .reset_index().set_index('trade_date').sort_index()[OREDER_COLS].tail(10)

    # 在手交易
    last_orders = orders.groupby(level=0).tail(1)
    holding = last_orders[last_orders.total_vol > 0]
    # 把 close 列换为今日结束报价并重新计算
    holding = holding.drop(columns=['close']).join(price.xs(end_date, level='trade_date', drop_level=True)['close'])
    holding.loc[:,'hold_value'] = holding.total_vol * holding.close
    holding.loc[:,'profit'] = holding.total_expense + holding.hold_value
    holding[OREDER_COLS]

    wins = finished[finished.profit>0]
    loses = finished[finished.profit<=0]
    print(f'From {start_date} to {end_date} summary:')
    print(f'Total profit: {cn_round_price(finished.profit.sum())}, Max profit: {round(finished.profit.max(),2)}, Max lost: {round(finished.profit.min(), 2)}.')
    print(f'Winning rate: {to_pct(len(wins)/len(finished))}% ({len(wins)}/{len(finished)}), Avg profit: {cn_round_price(wins.profit.sum()/len(wins))}, Avg loses: {cn_round_price(loses.profit.sum()/len(wins))}')
    print(f'Current holding: {len(holding)} stocks [￥{holding.hold_value.sum()}]. Current profit: {cn_round_price(holding.profit.sum())}.')

    return finished, holding, orders


def write_log_dep(date, comment, pred_dir, code=None, ts_code=None, name=None, pred_chg_1=None, dtype='BASIC', force_update=False):
    if name is not None:
        q_code = get_ts_code_from_name(name)
    elif code is not None:
        q_code = add_postfix(code)
    else:
        q_code = ts_code

    m = db_session.query(Memo).filter_by(ts_code=q_code, trade_date=date).first()
    if m is not None:
        q = db_session.query(Memo).filter_by(trade_date=date, ts_code=q_code)
        # df = pd.read_sql(q.statement, engine)
        df = read_sql(q.statement)
        display(df)
        if not force_update:
            return df
    else:
        m = Memo(ts_code=q_code, trade_date=date)

    m.comment = comment
    m.pred_dir= pred_dir
    m.dtype=dtype
    m.pred_chg_1 = m.pred_chg_1

    try:
        db_session.add(m)
        db_session.commit()
    except Exception as e:
        raise e
    finally:
        db_session.rollback()


#####################################################################
# Plate Analyze Helper
#####################################################################

def calc_plate_ranking(df, date, plate_detail, plate_type='concept', verbose=False):
    summary = """
    计算板块个股表现：涨停个股数量，平均涨幅，上涨 vs 下跌个数
    """
    if verbose:
        print(summary)

    if plate_type == 'concept':
        plate_code_col = 'concept_code'
        plate_name_col = 'concept_name'
    else:
        plate_code_col = 'ind_code'
        plate_name_col = 'ind_name'

    plate_codes = plate_detail[plate_code_col].unique()
    plates = plate_detail.groupby([plate_code_col, plate_name_col]).size().reset_index().rename(columns={0:'count'})
    plates.set_index(plate_code_col, inplace=True)
    for i in trange(len(plate_codes)):
        plate_code = plate_codes[i]
        stocks = plate_detail[plate_detail[plate_code_col] == plate_code].ts_code.to_list()
        try:
            df1 = df.loc[stocks].xs(date, level=1, drop_level=False)
        except KeyError as ke:
            print(ke)
            print(list(map(get_ts_code, stocks)))
            continue
        inc_cnt   = len(df1[df1.pct_chg > 0]) # 上涨个股数
        dec_cnt   = len(df1[df1.pct_chg < 0])
        flt_cnt   = len(df1[df1.pct_chg == 0])
        upbig_cnt = len(df1[df1.pct_chg > 7])
        upst_cnt  = len(df1[df1.limit == 'U'])
        dnst_cnt  = len(df1[df1.limit == 'D'])
        avg = df1.pct_chg.mean()
        plates.loc[plate_code, 'inc_cnt'] = inc_cnt
        plates.loc[plate_code, 'dec_cnt'] = dec_cnt
        plates.loc[plate_code, 'flt_cnt'] = flt_cnt
        plates.loc[plate_code, 'upbig_cnt'] = upbig_cnt
        plates.loc[plate_code, 'upst_cnt'] = upst_cnt
        plates.loc[plate_code, 'dnst_cnt'] = dnst_cnt
        plates.loc[plate_code, 'avg'] = avg
        plates.sort_values('upbig_cnt', inplace=True, ascending=False)
    return plates.dropna().astype({"inc_cnt":int, "dec_cnt":int, "inc_cnt":int, "flt_cnt":int, "upbig_cnt":int, "upst_cnt":int, "dnst_cnt":int, "avg":float})


def calc_plate_data(df, cons):
    summary = """
    New: 计算板块个股表现：涨停个股数量，平均涨幅，上涨 vs 下跌个数
    """
    cons_detail = (
        df.reset_index()[
            ['name', 'vol','amount','close','open','high','low', 'pct_chg', 'open_pct',
             'limit', 'upstop_num', 'trade_date','ts_code']]
          .merge(cons[['plate_name', 'plate_type','ts_code']], on='ts_code')
         .set_index(['ts_code','trade_date'])
    )
    cons_detail.loc[:,'up'] = cons_detail.pct_chg.map(lambda p: 1 if p > 0 else 0)
    cons_detail.loc[:,'dn'] = cons_detail.pct_chg.map(lambda p: 1 if p < 0 else 0)
    cons_detail.loc[:,'fl'] = cons_detail.pct_chg.map(lambda p: 1 if p == 0 else 0)
    tmp = (
        cons_detail.groupby(['trade_date', 'plate_type', 'plate_name'])
        .agg({'pct_chg':'mean', 'amount':'sum', 'upstop_num':'sum', 'close':'count', 'up':'sum', 'dn': 'sum', 'fl':'sum'})
        .reset_index().set_index(['plate_name', 'trade_date', 'plate_type']).sort_index()
    )
    tmp.loc[:, 'p1_pct_chg'] = tmp.groupby('plate_name').pct_chg.shift(1)
    tmp.loc[:, 'p3_pct_chg'] = tmp.groupby('plate_name').pct_chg.rolling(window=3).sum().values
    tmp.loc[:, 'p5_pct_chg'] = tmp.groupby('plate_name').pct_chg.rolling(window=5).sum().values

    # extract top stocks into plate summary dataframe
    top_detail = cons_detail[(cons_detail.pct_chg>=7) & (cons_detail.limit != 'U')]
    top_detail.loc[:,'top_stocks'] = top_detail.groupby(['trade_date', 'plate_type', 'plate_name']).name.transform(lambda x : ' '.join(x))

    # extract upstop stocks
    upstop_detail = cons_detail[cons_detail.limit=='U']
    upstop_detail.loc[:,'upstop_stocks'] = upstop_detail.groupby(['trade_date', 'plate_type', 'plate_name']).name.transform(lambda x : ' '.join(x))

    tmp = (
            tmp.join(upstop_detail.groupby(['trade_date', 'plate_type', 'plate_name']).first()[['upstop_stocks']])
               .join(top_detail.groupby(['trade_date', 'plate_type', 'plate_name']).first()[['top_stocks']])
          )
    con_sum = tmp.xs('concept', level='plate_type', drop_level=True)
    ind_sum = tmp.xs('industry', level='plate_type', drop_level=True)
    return con_sum, ind_sum, cons_detail


def calc_concept_ranking(df, date, concept_detail, verbose=False):
    summary = """
    计算板块个股表现：涨停个股数量，平均涨幅，上涨 vs 下跌个数
    """
    if verbose:
        print(summary)
    concept_codes = concept_detail.concept_code.unique()
    concepts = concept_detail.groupby(['concept_code', 'concept_name']).size().reset_index().rename(columns={0:'count'})
    concepts.set_index('concept_code', inplace=True)
    for i in trange(len(concept_codes)):
        concept_code = concept_codes[i]
        stocks = concept_detail[concept_detail.concept_code == concept_code].ts_code.to_list()
        try:
            df1 = df.loc[stocks].xs(date, level=1, drop_level=False)
        except KeyError as ke:
            print(ke)
            print(list(map(get_ts_code, stocks)))
            continue
        inc_cnt = len(df1[df1.pct_chg > 0]) # 上涨个股数
        dec_cnt = len(df1[df1.pct_chg < 0])
        flt_cnt = len(df1[df1.pct_chg == 0])
        upst_cnt = len(df1[df1.pct_chg >= 9.91])
        dnst_cnt = len(df1[df1.pct_chg <= -9.91])
        avg = df1.pct_chg.mean()
        concepts.loc[concept_code,'inc_cnt'] = inc_cnt
        concepts.loc[concept_code,'dec_cnt'] = dec_cnt
        concepts.loc[concept_code,'flt_cnt'] = flt_cnt
        concepts.loc[concept_code,'upst_cnt'] = upst_cnt
        concepts.loc[concept_code,'dnst_cnt'] = dnst_cnt
        concepts.loc[concept_code,'avg'] = avg
        concepts.sort_values('avg', inplace=True, ascending=False)
    return concepts.dropna().astype({"inc_cnt":int, "dec_cnt":int, "inc_cnt":int, "flt_cnt":int, "upst_cnt":int, "dnst_cnt":int, "avg":float})


def calc_concept_stock_rank(df_ori, top_con, cons_dts, trade_date=None, top_con_num=3, top_stock_num=5):
    """
    JoinQuant 传入df_ori, 排序好的概念 top_con, 概念和个股映射表 cons_dts, 返回一组 df，cons_dts
    """
    reports = {}
    if trade_date is None:
        trade_date = df_ori.index[-1][1]
        print(trade_date)
    for r in top_con.iloc[:top_con_num].iterrows():
        print(r[0])
        con_name = r[1]['concept_name']
        con_code = r[0]
        print(f'正在为 {con_code}: {con_name} ({r[1].avg})计算个股涨跌幅. \
              涨停个股: {r[1].upst_cnt}, 跌停个股: {r[1].dnst_cnt}, \
              涨平跌比例: {r[1].inc_cnt}/{r[1].flt_cnt}/{r[1].dec_cnt}')
        ts_codes = cons_dts[cons_dts.concept_code == con_code].ts_code.tolist()
        df = df_ori[(df_ori.index.isin(ts_codes, level='ts_code'))].swaplevel().loc[trade_date,].copy()
        reports[con_name] = df.sort_values('pct_chg', ascending=False).head(top_stock_num)
    return reports


def gen_concept_best_stocks(df_ori, top_con, cons_dts, trade_date=None, top_con_num=3, top_stock_num=5):
    """
    Tushare 传入df_ori, 排序好的概念 top_con, 概念和个股映射表 cons_dts, 返回一组 df，每组包含一个概念和这个概念里当日最强的个股
    """
    reports = {}
    if trade_date is None:
        trade_date = df_ori.index[-1][1]
        print(trade_date)
    for r in top_con.iloc[:top_con_num].iterrows():
        con_name = r[1]['name']
        con_code = r[1]['code']
        print(f'正在为 {con_name}, {con_code} 计算个股涨跌幅...')
        ts_codes = cons_dts.loc[con_code].ts_code.tolist()
        df = df_ori[(df_ori.index.isin(ts_codes, level='ts_code'))].swaplevel().loc[trade_date,].copy()
        reports[con_name] = df.sort_values('pct_chg', ascending=False).head(top_stock_num)
    return reports


#####################################################################
# Upstop Calcutators
#####################################################################

def calc_yinyang(df):
    summary = '''
        Calculate 连阳连阴数
    '''
    df.loc[:, 'yin'] = df.apply(lambda row: 1 if row.open*0.99 > row.close else 0, axis=1)
    df.loc[:, 'yang'] = df.apply(lambda row: 1 if row.open*1.01 < row.close else 0, axis=1)
    df.loc[:, 'yinyang'] = df.apply(lambda row: 'YIN' if row.open*0.99 > row.close else 'YANG' if row.open*1.01 < row.close else 'neutual', axis=1)
    tmpyang = df.yang.cumsum()
    df.loc[:, 'conseq_yang_num'] = tmpyang.sub(tmpyang.mask(df.yang == 1).ffill(), fill_value=0).astype(int)
    tmpyin = df.yin.cumsum()
    df.loc[:, 'conseq_yin_num'] = tmpyin.sub(tmpyin.mask(df.yin == 1).ffill(), fill_value=0).astype(int)
    return df


# Calculate 连板数, must have complete date index
def calc_upstop(df):
    # 当日是否涨停
    df.loc[:, 'upstop_num'] = df.limit.apply(lambda lim: 1 if lim == 'U' else -1 if lim =='D' else 0)

    # 累计连续涨停个数
    tmp = df.groupby(level='ts_code').upstop_num.cumsum()
    tmp2 = tmp.mask(df.upstop_num == 1).groupby(level='ts_code').ffill()

    df.loc[:, 'conseq_up_num'] = tmp.sub(tmp2, fill_value=0).astype(int)
    df.loc[:, 'pre_conseq_up_num'] = df.groupby('ts_code').conseq_up_num.shift(1)

    # 前日开板次数
    df.loc[:, 'pre_open_times'] = df.groupby('ts_code').open_times.shift(1)

    # TODO：几天几板
    df.loc[:, 'pre3_upstops'] = df.groupby(level='ts_code').upstop_num.apply(lambda x: x.rolling(window=3, min_periods=1).sum())
    df.loc[:, 'pre5_upstops'] = df.groupby(level='ts_code').upstop_num.apply(lambda x: x.rolling(window=5, min_periods=1).sum())
    df.loc[:, 'pre10_upstops'] = df.groupby(level='ts_code').upstop_num.apply(lambda x: x.rolling(window=10, min_periods=1).sum())
    df.loc[:, 'pre20_upstops'] = df.groupby(level='ts_code').upstop_num.apply(lambda x: x.rolling(window=20, min_periods=1).sum())

    # 未来3天连板数
    df.loc[:, 'post_up_num'] = df.groupby(level='ts_code').upstop_num.apply(
            lambda x: x.rolling(window=3, min_periods=1).sum().shift(-3)
        ) # .astype('Int64') # 用 Int64 可以处理 int 不能为 nan 的情况, 但是不能保存为 feather

    # 涨停类型
    df.loc[:, 'up_type'] = df[df.limit=='U'].apply(f_set_upstop_types, axis=1)
    df.loc[:, 'pre_up_type'] = df.groupby('ts_code').up_type.shift(1)
    df.loc[:, 'upstop_price'] = df.pre_close.apply(up_stop_price)

    return df


# 统计 df 内股票的平均涨幅和分布，用于计算涨停股次日的回报
def show_upstop_stat(df, bw_method='scott'):
    all_mean = round(df.pct_chg.mean(), 2)
    non_y_mean = round(df[df.pre_up_type != 'Y'].pct_chg.mean(), 2)
    print(f'全体平均涨幅：{all_mean}%，非一字板平均涨幅：{non_y_mean}%')
    for t in ['Y', 'T', 'H', 'W', 'L', 'O', 'M']:
        tmp = df[df.pre_up_type == t]
        print(f'{t}类板占比：{round(len(tmp)/len(df)*100, 0)}%，平均涨幅：{round(tmp.pct_chg.mean(), 2)}%')
    print(f'跌停票占比：{round(len(df[df.limit == "D"])/len(df)*100, 0)}%，涨停票占比：{round(len(df[df.limit == "U"])/len(df)*100, 0)}%')
    df[['pct_chg']].plot.kde(bw_method=bw_method)
    return df


# 分析一天的涨停股
def upstop_analyze(df, tdate, backtest=False, show_detail=True):
    print(f'Showing upstop stocks on {tdate}...')
    today_df = df.xs(slice(tdate, tdate), level='trade_date', drop_level=True)
    today_df.loc[:, 'c_v_o'] = today_df.pct_chg - today_df.open_pct
    if backtest:
        back_test_cols = ['next_limit', 'next_pct_chg']
    else:
        back_test_cols = []
    raw_today_up = today_df[today_df.limit == 'U']
    # today_up = today_df[today_df.limit == 'U'][
        # ['name', 'circ_mv', 'total_mv', 'conseq_up_num', 'pre_up_type', 'open_pct', 'up_type', 'c_v_o', 'pre_vol_type', 'pre_trf', 'turnover_rate_f'] + back_test_cols + [ 'vol', 'amount'] + ORI_COLS + ['ind_name', 'avg_pct_chg'] ]

    # today_drop = today_df[(today_df.pre_conseq_up_num >= 1) & (today_df.conseq_up_num == 0)][
        # ['name', 'circ_mv', 'total_mv', 'pre_conseq_up_num', 'pre_up_type', 'pre_open_times', 'open_pct', 'pct_chg', 'c_v_o', 'pre_vol_type', 'pre_trf', 'turnover_rate_f'] + back_test_cols + [ 'vol', 'amount'] + ['ind_name', 'avg_pct_chg'] ]

    today_up = today_df[today_df.limit == 'U'][
        ['name', 'circ_mv', 'total_mv', 'conseq_up_num', 'pre_up_type', 'auc_amt', 'open_pct', 'up_type', 'c_v_o', 'pre_vol_type', 'pre_trf', 'turnover_rate_f'] + back_test_cols + [ 'vol', 'amount'] + ORI_COLS]
    today_drop = today_df[(today_df.pre_conseq_up_num >= 1) & (today_df.conseq_up_num == 0)][
        ['name', 'circ_mv', 'total_mv', 'pre_conseq_up_num', 'pre_up_type', 'pre_open_times', 'auc_amt', 'open_pct', 'pct_chg', 'c_v_o', 'pre_vol_type', 'pre_trf', 'turnover_rate_f'] + back_test_cols + [ 'vol', 'amount']]

    print('连板个股')
    today_ups = today_up[(today_up.conseq_up_num >= 2)]
    display_up_df(today_ups.sort_values('conseq_up_num', ascending=False))

    print(f'涨停类型分布：')
    display(today_up.groupby('up_type').agg({'limit': 'count'}).transpose())

    # for up_type in ['Y', 'O', 'L', 'T', 'W', 'M']:
        # pct = len(today_up[today_up.up_type == up_type])/len(today_up) * 100
        # print(f'{up_type}: {len(today_up[today_up.up_type == up_type])}, 占 {round(pct, 2)}%')

    # print(f'掉队个股, 平均涨幅: {round(today_drop.pct_chg.mean(), 2)}%, c_v_o: {round(today_drop.c_v_o.mean(), 2)}%')

    pre_up = today_df[today_df.pre_conseq_up_num >= 1]
    print(f'前日涨停结果: 按涨停类型')
    pre_uptype_sum = pre_up.groupby('pre_up_type').agg({'open_pct': 'mean', 'pct_chg': 'mean', 'c_v_o': 'mean', 'upstop_num': 'sum', 'close': 'count'})
    display(style_df(pre_uptype_sum))

    print(f'前日涨停结果: 按量比情况')
    pre_voltype_sum = pre_up.groupby('pre_vol_type').agg({'open_pct': 'mean', 'pct_chg': 'mean', 'c_v_o': 'mean', 'upstop_num': 'sum', 'close': 'count'})
    display(style_df(pre_voltype_sum))
    # for vol_type in ['Ultra', 'Huge', 'Normal', 'Small', 'Tiny']:
        # show_df_stats(pre_up[pre_up.pre_up_type == up_type])
        # pct = len(today_ups[today_ups.pre_vol_type == vol_type])/len(today_ups) * 100
    #     print(f'{vol_type}: {len(today_ups[today_ups.pre_vol_type == vol_type])}, 占 {round(pct, 2)}%')

    if show_detail:
        print('最早上板')
        display_up_df(today_up.sort_values('first_time', ascending=True).head(5))

        print('最早封死')
        display_up_df(today_up.sort_values('last_time', ascending=True).head(5))

        print('封单 vs 成交最大')
        display_up_df(today_up.sort_values('fc_ratio', ascending=False).head(5))

        print('封单比例最大')
        display_up_df(today_up.sort_values('fl_ratio', ascending=False).head(5))

        print('成交额最大')
        display_up_df(today_up.sort_values('amount', ascending=False).head(5))

        print('一字&T字')
        display_up_df(today_up[(today_up.up_type == 'Y') | (today_up.up_type == 'T')].sort_values('conseq_up_num', ascending=False))

        print(f'掉队个股')
        display_up_df(today_drop.sort_values('pre_conseq_up_num', ascending=False))

    if backtest:
        print('次3日涨幅最大')
        display_up_df(today_up.sort_values('next3_pct_chg', ascending=False).head(5))

    return raw_today_up


#####################################################################
# Pandas Applicable Funcs
#####################################################################

def f_set_vol_types(row):
    if row.vol > row.ma_vol_20 * 5:
        return 'Ultra' # 爆量
    elif  row.vol > row.ma_vol_20 * 2:
        return 'Huge'  # 放量
    elif  row.vol > row.ma_vol_20 * 0.85:
        return 'Normal'      # 带量
    elif  row.vol > row.ma_vol_20 * 0.5:
        return 'Small'       # 缩量
    else:  # row.vol < row.ma_vol_20 * 0.5:
        return 'Tiny'        # 无量


def f_set_upstop_types(row):
    if row.limit == 'U':
        if row.low == row.close:
            return 'Y' # 一字板
        elif row.open == row.close and row.open > row.low:
            return 'T' # T 字板
        elif row.open < row.pre_close:
            return 'L' # 开盘水下拉涨停
        elif row.open/row.pre_close > 1.05 and row.open < row.close and row.open == row.low:
            return 'H' # 高开封板无回调
        elif row.high / row.low > 1.1:
            return 'W' # 波动超过 10%
        elif type(row.first_time) != str:
            return 'M' #  秒板
        else:
            return 'O' # other situation
    else:
        return 'N' # 未涨停


def f_calc_yinyang(row):
    if row.open*0.99 > row.close:
        return 'yin'
    elif row.open*1.01 < row.close:
        return 'yang'
    else:
        return 'cross'


if __name__ == '__main__':
    from utils.psql_client import load_stock_prices
    from utils.datetimes import month_ago_date, end_date
    df = load_stock_prices(month_ago_date, end_date)
    print(f'Calculating MA for {len(df)} rows...')
    df = gen_ma(df, mavgs=[5, 10])
    print(df[['high', 'low', 'close', 'ma_close_5', 'ma_close_10']].tail(5))
