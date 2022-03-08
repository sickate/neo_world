# matplotlib imports
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib import rc
from matplotlib import ticker
from matplotlib import style
from matplotlib.pylab import date2num
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()

# fix CN chars
mpl.matplotlib_fname()
from matplotlib import rcParams
from matplotlib.font_manager import FontProperties
FONT_ARIAL = FontProperties(fname='/Library/Fonts/Arial Unicode.ttf', size=15)
FONT_SONG = FontProperties(fname='/Library/Fonts/Songti.ttc', size=15)
rcParams['axes.unicode_minus'] = False
rcParams['figure.figsize'] = 16, 10

# mplfinance imports
from cycler import cycler# 用于定制线条颜色
import mplfinance as mpf


def plot_k(df, ts_code=None, type='candle', volume=True, mav=(5, 10, 30), figscale=1, verbose=False):
    summary = '''
        [mplfinance] Plot k 线图
    '''
    if verbose:
        print(summary)

    # 设置marketcolors
    # up:设置K线线柱颜色，up意为收盘价大于等于开盘价
    # down:与up相反，这样设置与国内K线颜色标准相符
    # edge:K线线柱边缘颜色(i代表继承自up和down的颜色)，下同。详见官方文档)
    # wick:灯芯(上下影线)颜色
    # volume:成交量直方图的颜色
    # inherit:是否继承，选填
    mc = mpf.make_marketcolors(
        up='red',
        down='green',
        inherit=True
#         edge='i', 
#         wick='i', 
#         volume='i', 
    )

    # 设置图形风格
    # gridaxis:设置网格线位置
    # gridstyle:设置网格线线型
    # y_on_right:设置y轴位置是否在右
    s = mpf.make_mpf_style(
        gridaxis='both',
        gridstyle='-.',
        y_on_right=False,
        marketcolors=mc)

    # 设置均线颜色，配色表可见下图
    # 建议设置较深的颜色且与红色、绿色形成对比
    # 此处设置七条均线的颜色，也可应用默认设置
    mpl.rcParams['axes.prop_cycle'] = cycler(
        color=['dodgerblue', 'deeppink', 'navy', 
               'teal', 'maroon', 'darkorange', 'indigo']
    )

    if ts_code is not None:
        df = df[df.ts_code == ts_code]

    if len(df) == 0:
        return

    df = df.sort_index(ascending=True)
    df = df[['open', 'close', 'high', 'low', 'vol']]
    df.columns = ['Open', 'Close', 'High', 'Low', 'Volume']
    if verbose:
        print(df.head())
        print(df.tail())
    mpf.plot(df, type=type, volume=True, mav=mav, show_nontrading=False, figscale=figscale,
             title='Stock Price', ylabel='OHLC Candles', ylabel_lower='Volume', style=s)


