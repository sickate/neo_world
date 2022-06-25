import sys
sys.path.append('./')

from models import *

from utils.pd_styler import *
from utils.calculators import *


#####################################################################
# Indicator Calculators
#####################################################################

def gen_adj_price(df, replace=False, single_index=False):
    summary = """
        This version works on Multiindex and single index(only 1 stock)
        * single_index =:
            True: 输入 Dataframe 只有一个 trade_date index，没有 ts_code
        * replace =:
              True: 在 close/open/high/low/pre_close 列原地替换
              False: 新建 5 列 adj_pre_close/adj_open ...
    """
    if single_index:
        last_factor = df.iloc[-1].at['adj_factor']
    else:
        last_factor = df.reset_index().groupby('ts_code').agg({'trade_date':'max', 'adj_factor': 'last'}).adj_factor
        ts_codes = df.index.get_level_values('ts_code')
        df.loc[:, 'last_factor'] = last_factor.loc[ts_codes].values

    df.loc[:, 'norm_adj'] =  df.adj_factor / df.last_factor
    for x in ['high', 'low', 'close', 'open', 'pre_close']:
        if replace:
            df.loc[:,x] =  (df[x] * df.norm_adj).apply(lambda p: cn_round_price(p))
        else:
            df.loc[:,f'adj_{x}'] = (df[x] * df.norm_adj).apply(lambda p: cn_round_price(p))

    df.loc[:,f'adj_vol'] = (df.vol/df.norm_adj).apply(lambda p: cn_round_price(p))
    return df


def gen_kdj(df, start_date, end_date, plot=False, cn_mode=True):
    summary = """
        只支持single index, need update
        cn_mode 使用 ema，非 cn_mode 使用 talib 的 sma 算法。本函数默认 close 是除权过的。
    """
    if cn_mode:
        # 计算KDJ指标,前9个数据为空
        low_list = df.loc[:,'low'].rolling(window=9).min()
        high_list = df.loc[:,'high'].rolling(window=9).max()

        rsv = (df.close - low_list) / (high_list - low_list) * 100
        df.loc[:,'k'] = rsv.ewm(com=2).mean()
        df.loc[:,'d'] = df.k.ewm(com=2).mean()
        df.loc[:,'j'] = 3 * df.k - 2 * df.d
    else:
        df.loc[:,'k'], df.loc[:,'d'] = ta.STOCH(df.high, df.low, df.close,
                                                fastk_period=9,
                                                slowk_period=3,
                                                slowk_matype=0,
                                                slowd_period=3,
                                                slowd_matype=0)
        df.loc[:,'j'] = 3* df.k - 2* df.d

    # 计算KDJ指标金叉、死叉情况
    df.loc[:,'kdj_sig'] = ''
    kdj_position = df.k > df.d
    df.loc[kdj_position[(kdj_position == True) & (kdj_position.shift() == False)].index, 'kdj_sig'] = 'jx'
    df.loc[kdj_position[(kdj_position == False) & (kdj_position.shift() == True)].index, 'kdj_sig'] = 'sx'
    if plot:
        plot_mline(df, legend_labels=['k','d','j'], title='KDJ', y_label='Value')

    return df


def gen_ma(df, mavgs=None, col='close', add_shift=0, single_index=False):
    '''
        Generate mav
    '''

    if mavgs is None:
        mavgs = [5, 10, 30]
    for d in mavgs:
        if single_index:
            df.loc[:, f'ma_{col}_{d}'] = df[col].rolling(window=d).mean().shift(add_shift)
        else:
            # df.loc[:, f'ma_{col}_{d}'] = df.groupby(level='ts_code')[col].rolling(window=d).mean().values
            df.loc[:, f'ma_{col}_{d}'] = df.groupby(level='ts_code')[col].apply(lambda x: x.rolling(window=d).mean().shift(add_shift))

    if col=='close':
        df.loc[:, 'hl_pct'] = (df['high'] - df['low']) / df[col] * 100.0
    return df


def gen_vwap(df, spans=None):
    if not spans:
        spans = [5, 10]
    for span in spans:
        vol_in_span = df.groupby(level='ts_code').adj_vol.apply(lambda x: x.rolling(window=span).sum())
        amt_in_span = df.groupby(level='ts_code').amount.apply(lambda x: x.rolling(window=span).sum())

        df.loc[:, f'vwap_{span}'] = (amt_in_span/vol_in_span).apply(lambda p: cn_round_price(p*10))
    return df


def gen_macd(df):
    summary = """
        仅支持 multiindex
        The MACD Line is the difference between fast EMA and slow EMA. (DIF)
        Signal line is 9 period EMA of the MACD Line. (DEA)
        MACD Histogram is the difference between MACD Line and Signal line. (bar)
    """
    tmp = df

    # 计算均线
    # tmp = gen_ma(tmp, mavgs=[5,10])

    # 计算均线金叉sig（5日上穿 10 日）
    # tmp.loc[:,'MA5_T1'] = tmp.groupby(level='ts_code').ma5.shift(1)
    # tmp.loc[:,'MA10_T1'] = tmp.groupby(level='ts_code').ma10.shift(1)
    # tmp.loc[:, 'MAVGX'] = (tmp.MA5_T1 < tmp.MA10_T1) & (tmp.ma5 > tmp.ma10)

    # 计算 macd
    ts_codes = tmp.index.unique(level='ts_code')
    tmpall = pd.DataFrame()
    for ts_code in ts_codes:
        tmp_macd =  tmp.loc[ts_code][['close']].reset_index()
        if len(tmp_macd) <= 33:
            continue
        tmp_macd.loc[:, 'ts_code'] = ts_code
        tmp_macd.set_index(['ts_code','trade_date'], inplace=True)
        try:
            tmp_macd.loc[:,'DIF'], tmp_macd.loc[:,'DEA'], tmp_macd.loc[:,'MACD'] = ta.MACD(tmp_macd.close, fastperiod=12, slowperiod=26, signalperiod=9)
            tmpall = tmpall.append(tmp_macd)
        except:
            print(ts_code)

    tmp.loc[:,'DIF'] = tmpall.DIF
    tmp.loc[:,'DEA'] = tmpall.DEA
    tmp.loc[:,'MACD'] = tmpall.MACD

    # 计算 macd 相关 signals
    # MACD 红柱增加： t-2 日 > t-1 日 > t 日 且 t-2 日 > 0
    tmp.loc[:,'BAR_T2'] = tmp.groupby(level='ts_code').MACD.shift(2)
    tmp.loc[:,'BAR_T1'] = tmp.groupby(level='ts_code').MACD.shift(1)
    tmp.loc[:, 'MACDBAR'] = (tmp.BAR_T2 < tmp.BAR_T1) & (tmp.BAR_T1 < tmp.MACD) & (tmp.BAR_T2 > 0)
    # MACD 0 上金叉：
    tmp.loc[:,'DEA_T1'] = tmp.groupby(level='ts_code').DEA.shift(1)
    tmp.loc[:,'DIF_T1'] = tmp.groupby(level='ts_code').DIF.shift(1)
    tmp.loc[:, 'MACDGX1'] = (tmp.DIF > 0) & (tmp.DEA > 0) & (tmp.DIF > tmp.DEA) & (tmp.DIF_T1 < tmp.DEA_T1)
    # MACD 0 下金叉：
    tmp.loc[:, 'MACDGX2'] = (tmp.DIF < 0) & (tmp.DEA < 0) & (tmp.DIF > tmp.DEA) & (tmp.DIF > tmp.DEA) & (tmp.DIF_T1 < tmp.DEA_T1)
    return tmp


def gen_macd_single_index(df):
    summary = """
        The MACD Line is the difference between fast EMA and slow EMA. (DIF)
        Signal line is 9 period EMA of the MACD Line. (DEA)
        MACD Histogram is the difference between MACD Line and Signal line. (bar)
    """
    tmp = df

    # 计算均线
    tmp = gen_ma(tmp, mavgs=[5, 10])
    # 计算均线金叉sig（5日上穿 10 日）
    tmp.loc[:, 'MAVGX'] = (
        (tmp.groupby(level='ts_code').ma5.shift(1) < tmp.groupby(level='ts_code').ma10.shift(1))
        & (tmp.groupby(level='ts_code').ma5 > tmp.groupby(level='ts_code').ma10)
    )

    # 计算 macd
    tmp.loc[:,'DIF'], tmp.loc[:,'DEA'], tmp.loc[:,'MACD'] = ta.MACD(tmp.close, fastperiod=12, slowperiod=26, signalperiod=9)
    # 计算 macd 相关 signals
    # MACD 红柱增加： t-2 日 > t-1 日 > t 日 且 t-2 日 > 0
    tmp.loc[:, 'MACDBAR'] = (tmp.MACD.shift(2) < tmp.MACD.shift(1)) & (tmp.MACD.shift(1) < tmp.MACD) & (tmp.MACD.shift(2) > 0)
    # MACD 0 上金叉：
    tmp.loc[:, 'MACDGX1'] = (tmp.DIF > 0) & (tmp.DEA > 0) & (tmp.DIF > tmp.DEA) & (tmp.DIF.shift(1) < tmp.DEA.shift(1))
    # MACD 0 下金叉：
    tmp.loc[:, 'MACDGX2'] = (tmp.DIF < 0) & (tmp.DEA < 0) & (tmp.DIF > tmp.DEA) & (tmp.DIF.shift(1) < tmp.DEA.shift(1))

    return tmp


