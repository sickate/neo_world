import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from utils.datetimes import trade_day_util as tdu
from utils.type_helpers import *

from textwrap import wrap
named_colorscales = px.colors.named_colorscales()

css_palette = [
 'aqua',
 'brown',
 'darkcyan',
 'azure',
 'beige',
 'bisque',
 'black',
 'blanchedalmond',
 'blue',
 'blueviolet',
 'burlywood',
 'aquamarine',
 'cadetblue',
 'chartreuse',
 'chocolate',
 'coral',
 'cornflowerblue',
 'cornsilk',
 'crimson',
 'cyan',
 'darkblue',
 'darkgoldenrod',
 'darkgray',
 'darkgrey',
 'darkgreen',
 'darkkhaki',
 'darkmagenta',
 'darkolivegreen',
 'darkorange',
 'darkorchid',
 'darkred',
 'darksalmon',
 'darkseagreen',
 'darkslateblue',
 'darkslategray',
 'darkslategrey',
 'darkturquoise',
 'darkviolet',
 'deeppink',
 'deepskyblue',
 'dimgray',
 'dimgrey',
 'dodgerblue',
 'firebrick',
 'floralwhite',
 'forestgreen',
 'fuchsia',
 'gainsboro',
 'ghostwhite',
 'gold',
 'goldenrod',
 'gray',
 'grey',
 'green',
 'greenyellow',
 'honeydew',
 'hotpink',
 'indianred',
 'indigo',
 'ivory',
 'khaki',
 'lavender',
 'lavenderblush',
 'lawngreen',
 'lemonchiffon',
 'lightblue',
 'lightcoral',
 'lightcyan',
 'lightgoldenrodyellow',
 'lightgray',
 'lightgrey',
 'lightgreen',
 'lightpink',
 'lightsalmon',
 'lightseagreen',
 'lightskyblue',
 'lightslategray',
 'lightslategrey',
 'lightsteelblue',
 'lightyellow',
 'lime',
 'limegreen',
 'linen',
 'magenta',
 'maroon',
 'mediumaquamarine',
 'mediumblue',
 'mediumorchid',
 'mediumpurple',
 'mediumseagreen',
 'mediumslateblue',
 'mediumspringgreen',
 'mediumturquoise',
 'mediumvioletred',
 'midnightblue',
 'mintcream',
 'mistyrose',
 'moccasin',
 'navajowhite',
 'navy',
 'oldlace',
 'olive',
 'olivedrab',
 'orange',
 'orangered',
 'orchid',
 'palegoldenrod',
 'palegreen',
 'paleturquoise',
 'palevioletred',
 'papayawhip',
 'peachpuff',
 'peru',
 'pink',
 'plum',
 'powderblue',
 'purple',
 'red',
 'rosybrown',
 'royalblue',
 'rebeccapurple',
 'saddlebrown',
 'salmon',
 'sandybrown',
 'seagreen',
 'seashell',
 'sienna',
 'silver',
 'skyblue',
 'slateblue',
 'slategray',
 'slategrey',
 'snow',
 'springgreen',
 'steelblue',
 'tan',
 'teal',
 'thistle',
 'tomato',
 'turquoise',
 'violet',
 'wheat',
 'white',
 'whitesmoke',
 'yellow',
 'yellowgreen'
]

def plot_k_plotly(df_in, verbose=False, ma_spans=[5, 10], ma_col='ma_close', height=600):
    summary = '''
        [plotly] Plot Candlestick
    '''
    if verbose:
        print(summary)

    df = df_in.copy(deep=True)
    df.loc[:,'hovertext'] = df.apply(f_get_hovertext, axis=1)

    if 'name' in df.columns:
        title = df.name.unique()[0]
    else:
        title = 'OHLC Chart'

    start_date = df.index[0].strftime('%Y-%m-%d')
    end_date = df.index[-1].strftime('%Y-%m-%d')

    # Create subplots and mention plot grid size
    fig = make_subplots(
            rows=3, cols=1, shared_xaxes=True, 
            vertical_spacing=0.03, 
            subplot_titles=(f'{title}: {start_date} - {end_date}', 'Volume'), 
            row_width=[0.2, 0.2, 0.7]
        )

    # Plot OHLC on 1st row
    fig.add_trace(go.Candlestick(
                            x=df.index,
                            open=df.open,
                            close=df.close,
                            high=df.high,
                            low=df.low,
                            increasing_line_color= 'red', 
                            decreasing_line_color= 'green',
                            text=df.hovertext,
                            hoverinfo='text',
                            name=title
                        ),row=1, col=1)

    # ma_colors = ["#3ffced", "#fff94d"]
    ma_colors = css_palette

    for i, ma_span in enumerate(ma_spans):
        ma_settings = {
            "line": {"width": 1}, 
            "meta": {"columnNames": 
                {
                    "x": "Moving Average, x", 
                    "y": "Moving Average, y"
                }
            }, 
            "mode": "lines", 
            "name": f"{ma_col}{ma_span}", 
            "type": "scatter", 
            "x": df.index,
            "y": df[f'{ma_col}_{ma_span}'],
            "yaxis": "y2", 
            "marker": {"color": ma_colors[i]}
        }
        fig.add_trace(ma_settings, row=1, col=1)

    # Bar trace for volumes on 2nd row without legend
    fig.add_trace(go.Bar(x=df.index, y=df.vol, showlegend=True, name='Vol'), row=3, col=1)

    start_date = df.index[0].strftime('%Y-%m-%d')
    end_date = df.index[-1].strftime('%Y-%m-%d')
    fig.update_xaxes(
        rangebreaks=[
            # dict(bounds=["sat", "mon"]), #hide weekends
            dict(values=tdu.non_trading_days(start_date, end_date))
        ]
    )

    # set size
    fig.update_layout(
        autosize=True,
        height=height)

    # Do not show OHLC's rangeslider plot 
    # fig.update(layout_xaxis_rangeslider_visible=False)
    fig.show()


def plot_emo_trend(df):
    summary = '''
        [plotly] 绘制每日涨停情绪图
    '''
    fig = make_subplots(rows=2, cols=1, row_heights=[0.7, 0.3], specs=[[{"secondary_y": True}], [{"secondary_y": True}]])

    fig.add_trace(
        go.Scatter(x=df.index, y=df.pre_up_pct.round(2), name='昨日涨停股今平均涨幅'), row=1, col=1, secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(x=df.index, y=df.pre_ups_pct.round(2), name='昨日连板股今平均涨幅'), row=1, col=1, secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(x=df.index, y=df.p_up_t_noup_pct.round(2), name='掉队股平均涨幅'), row=1, col=1, secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(x=df.index, y=df.pre_up_cons_pct.round(2), name='昨涨停晋级率'), row=2, col=1, secondary_y=True,
    )
    fig.add_trace(
        go.Bar(x=df.index, y=df.upst_cnt, name='涨停个股数'), row=2, col=1, secondary_y=False,
    )

    fig.add_trace(
        go.Bar(x=df.index, y=df.cons_upst_cnt, name='连板个股数'), row=2, col=1, secondary_y=False,
    )

    start_date = df.index[0]
    end_date = df.index[-1]
    fig.update_xaxes(
        rangebreaks=[
            dict(values=tdu.non_trading_days(start_date, end_date))
        ]
    )
    fig.update_layout(height=600, title_text="Emotions")
    # fig.show()
    return fig


###########################################
# Helper functions
###########################################

def f_get_hovertext(row):
    text = f"""
{row.name.strftime('%Y-%m-%d')}<br>
PCT: {row.pct_chg}%<br>
High: {row.high}<br>
Low: {row.low}
Vol: {row.vol}手
"""
    return text
