import pandas as pd
import backtrader as bt
import backtrader.indicators as btind
import backtrader.feeds as btfeeds

class MyPandasFeed(btfeeds.PandasData):
    params = (
        # Possible values for datetime (must always be present)
        #  None : datetime is the "index" in the Pandas Dataframe
        #  -1 : autodetect position or case-wise equal name
        #  >= 0 : numeric index to the colum in the pandas dataframe
        #  string : column name (as index) in the pandas dataframe
        ('datetime', None),

        # Possible values below:
        #  None : column not present
        #  -1 : autodetect position or case-wise equal name
        #  >= 0 : numeric index to the colum in the pandas dataframe
        #  string : column name (as index) in the pandas dataframe
        ('open', -1),
        ('high', -1),
        ('low', -1),
        ('close', -1),
        ('volume', -1),
        ('openinterest', -1),
        ('pct_chg', -1),
        ('turnover_rate_f', -1),
        ('pre5_upstops', -1),
        ('pre10_upstops', -1),
    )

    lines = ('pct_chg', 'turnover_rate_f', 'pre5_upstops', 'pre10_upstops')

def make_bt_df(df, minimum=False):
    if minimum:
        bt_df = df[['open','close','high','low','vol']]
    else:
        bt_df = df
    bt_df.rename(columns={'vol': 'volume'})
    bt_df.loc[:, 'openinterest'] = 0
    bt_df.index = pd.to_datetime(bt_df.index)
    return bt_df


# 填充 df
def fill_df(df, trade_days, verbose=False):
    # df 中的第一天应该是上市日期
    list_day_in_ts = df.index[0]
    for day in trade_days:
        day_in_ts = pd.to_datetime(day)
        if day_in_ts not in df.index:
            df.loc[day_in_ts, 'volume'] = 0
            if day_in_ts < list_day_in_ts:
                df.loc[day_in_ts, 'open'] = 0
                df.loc[day_in_ts, 'close'] = 0
                df.loc[day_in_ts, 'low'] = 0
                df.loc[day_in_ts, 'high'] = 0
                df.loc[day_in_ts, 'openinterest'] = 0
            else:
                if verbose:
                    print(f'Missing data for {day}')
    df.fillna(axis=0, method='backfill', inplace=True) # 按列向前填充缺失数据
    return df.sort_index()

