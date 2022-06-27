import pandas as pd
import seaborn as sns

from utils.type_helpers import *

def color_negative_red(val):
    """
    Takes a scalar and returns a string with
    the css property `'color: red'` for negative
    strings, black otherwise.
    """
    if isinstance(val, str):
        return val
    color = 'red' if val > 0 else 'green'
    return 'color: %s' % color


def highlight_max(s):
    is_max = s == s.max()
    return ['background-color: yellow' if v else '' for v in is_max]


def style_group_summary(df):
    return (
        df.style.bar(subset=['avg_pct_chg'], align='zero', color=['limegreen', 'crimson'])
            .format('{:.3}%',  subset=['avg_pct_chg'])
            .format('{:,.0f}', subset=['inc_cnt', 'dec_cnt', 'flt_cnt', 'upst_cnt', 'dnst_cnt'])
            .format('{:,.0f}亿', subset=['amt_ttl'])
    )


def style_stock_df(df):
    pct_cols = ['open_pct', 'pct_chg', 'pct_chg_in_5']
    pct_subset = intersection(pct_cols, df.columns)
    pct_amount = intersection(['amount'], df.columns)
    return (
        df.style.bar(subset=pct_subset, align='zero', color=['limegreen', 'crimson'])
            .format('{:.3}%',  subset=pct_subset)
            .format('{:,.0f}千', subset=pct_amount)
    )


def format_df(df):
    pct_cols = ['open_pct', 'pct_chg', 'c_v_o', 'turnover_rate_f', 'net_pct_main', 'net_trf_main', 'avg_pct_chg']
    pct_subset = intersection(pct_cols, df.columns)
    ratio_subset = intersection(['auc_ratio_all', 'auc_vol_ratio', 'vol_ratio'], df.columns)
    return (
        df.style.format('{:.3}%',  subset=pct_subset)
            .format('{:.3}', subset=ratio_subset)
    )


def style_df(df):
    # 使用 Seaborn 颜色格式化单个股票一段时期内 dataframe 的式样
    pct_cols = ['open_pct', 'pct_chg', 'next_pct_chg', 'next2_pct_chg', 'c_v_o', 'turnover_rate_f', 'net_pct_main', 'net_trf_main', 'avg_pct_chg']

    pct_subset = intersection(pct_cols, df.columns)
    cmo_subset = intersection(['auc_ratio_all', 'net_mf_vol', 'amt_ttl'], df.columns)
    cmg_subset = intersection(['auc_vol_ratio', 'vol_ratio', 'vol', 'turnover_rate_f', 'auc_vol'], df.columns)
    ratio_subset = intersection(['auc_ratio_all', 'auc_vol_ratio', 'vol_ratio'], df.columns)

    cmg = sns.light_palette("green", as_cmap=True)
    cmo = sns.light_palette("orange", as_cmap=True)
    cmpct = sns.diverging_palette(240, 10, as_cmap=True)
    return (
        df.style.format('{:.3}%',  subset=pct_subset)
            .format('{:.3}', subset=ratio_subset)
            .background_gradient(cmap=cmpct, subset=pct_subset)
            .background_gradient(cmap=cmg, subset=cmo_subset)
            .background_gradient(cmap=cmo, subset=cmg_subset)
    )


def style_full_df(df):
    cmpct = sns.diverging_palette(240, 10, as_cmap=True)
    cmo = sns.light_palette("orange", as_cmap=True)

    df.loc[:,'circ_mv']=df.circ_mv/10000
    if 'total_mv' in df.columns.to_list():
        df.loc[:,'total_mv']=df.total_mv/10000
    df.loc[:,'amount']=df.amount/100000
    if 'fd_amount' in df.columns.to_list():
        df.loc[:,'fd_amount']=df.fd_amount/100000000
    if 'pre_amount' in df.columns.to_list():
        df.loc[:,'pre_amount']=df.pre_amount/100000
    if 'vol' in df.columns.to_list():
        df.loc[:,'vol']=df.vol/10000
    if 'auc_amt' in df.columns.to_list():
        df.loc[:,'auc_amt']=df.auc_amt/100000
    if 'dde_amt' in df.columns.to_list():
        df.loc[:,'dde_amt']=df.dde_amt/100000
    if 'next_auc_amt' in df.columns.to_list():
        df.loc[:,'next_auc_amt']=df.next_auc_amt/100000
    # df.avg_pct_chg.fillna(0, inplace=True)
    df = df.rename(columns={
        'turnover_rate_f': '流动换手率',
        'conseq_up_num': '连板数',
        'pre_conseq_up_num': '前日连板数',
        'amount': '成交额',
        'pre_amount': '前日金额',
        'auc_amt': '竞价金额',
        'next_auc_amt': '次日竞价金额',
        'circ_mv': '流值',
        'total_mv': '总市值',
        'open_pct': '开盘涨幅',
        'pct_chg': '涨幅',
        'next_open_pct': '次开涨幅',
        'vol_ratio': '量比',
        'next_pct_chg': '次日涨幅',
        'fd_amount': '封单金额',
        'auc_pre_vol_ratio': '竞价成交比',
        'next_auc_pvol_ratio': '次日竞成比',
        # 'avg_pct_chg': '板块涨幅',
    })

    money_cols = intersection(df.columns, ['成交额', '流值', '总市值', '封单金额', '竞价金额', '前日金额', '次日竞价金额', 'dde_amt'])
    price_cols = intersection(df.columns, ['open', 'close', 'high', 'low', 'strth'])
    vol_cols = intersection(df.columns, ['vol'])
    pct_cols = intersection(df.columns, ['流动换手率', '涨幅', '开盘涨幅', '次开涨幅', '次日涨幅', 'c_v_o', 'pre_trf', 'fc_ratio', 'fl_ratio', 'dde'])
    grad_ct_cols = intersection(df.columns, ['涨幅', '开盘涨幅', '次开涨幅', '次日涨幅'])
    grad_o_cols = intersection(df.columns, ['流动换手率', 'pre_trf'])
    float_3_cols = intersection(df.columns, ['竞价成交比', '次日竞成比', '量比'])
    int_cols = intersection(df.columns, ['open_times', '连板数', '前日连板数', ])

    # return df
    df_styled = (
        df.style.format('{:.2f}亿',  subset=money_cols)
                .format('{:.1f}万手', subset=vol_cols)
                .format('{:.0f}', subset=int_cols)
                .format('{:.2f}', subset=price_cols)
                .format('{:.3f}', subset=float_3_cols)
                .format('{:.2f}%', subset=pct_cols)
                .background_gradient(cmap=cmpct, subset=grad_ct_cols, vmin=-11, vmax=11)
                .background_gradient(cmap=cmo, subset=grad_o_cols)
                .background_gradient(cmap=cmo, subset=float_3_cols, vmin=0.02, vmax=0.5)
                # .format('{:.2f}%', subset=['流动换手率', '板块涨幅', '涨幅', '开盘涨幅', '次日涨幅', 'c_v_o', 'pre_trf'])
                # .background_gradient(cmap=cmpct, subset=['板块涨幅', '涨幅', '开盘涨幅', '次日涨幅'])
    )
    return df_styled


def display_up_df(df):
    display(style_full_df(df))
    return None



