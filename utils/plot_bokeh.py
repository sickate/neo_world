import bokeh.plotting as bplt
from bokeh.plotting import figure
from bokeh.io import show, output_notebook, output_file
from bokeh.palettes import Category20_20, Category20_16, inferno, magma
from bokeh.layouts import column, row, WidgetBox
from bokeh.models import Legend, Panel, ColumnDataSource, HoverTool, DatetimeTickFormatter
from bokeh.models.widgets import Tabs, CheckboxGroup, Slider, RangeSlider


def plot_mline(df, legend_labels, title, y_label, datespan=None, colors=None, w=800, h=450, verbose=False):
    summary = '''
        [Bokeh] 绘制单一 ts_code 的多指标线, 指标来自多个 column
    '''
    if verbose:
        print(summary)
    p = figure(plot_width=w, plot_height=h, title=title,
               x_axis_label= '交易日期', y_axis_label=y_label)
    p.xaxis[0].formatter = DatetimeTickFormatter()

    colors = Category20_16 if colors is None else colors
    legend_it=[]

    for legend, color in zip(legend_labels, colors):
        if datespan is not None:
            df=df[datespan].copy()
        c = p.line(df.index, df.loc[:,legend], line_width=2, color=color)
        legend_it.append((legend, [c]))

    legend = Legend(items=legend_it, location=(0, -60), click_policy='mute')
    p.add_layout(legend, 'right')
    show(p)


def plot_multindex_line(df, y_label, title=None, ts_codes=None, datespan=None, colors=None, w=800, h=450, verbose=False):
    summary = '''
        [Bokeh] 绘制多 ts_code 的单一指标线, 数据来自 multiindex: tuple(ts_code, datetime)
    '''
    if verbose:
        print(summary)
    p = figure(plot_width=w, plot_height=h,
               title = title,
               x_axis_label = df.index.names[1], y_axis_label = y_label)
    p.xaxis[0].formatter = DatetimeTickFormatter()
    p.left[0].formatter.use_scientific = False

    if ts_codes is not None:
        df = df[df.index.isin(ts_codes, level='ts_code')]

    colors = Category20_20 if colors is None else colors
    legend_it=[]

    for line, color in zip(df.index.get_level_values(0).unique().to_list(), colors):
        tmpdf = df.loc[line]
        if datespan is not None:
            tmpdf=tmpdf[datespan].copy()
        c = p.line(tmpdf.index, tmpdf.loc[:, y_label], line_width=2, color=color)
        legend_it.append((line, [c]))

    legend = Legend(items=legend_it, location=(0, 0), click_policy='mute')
    p.add_layout(legend, 'right')
    show(p)


# 这个function 和上一个多有相似
def plot_mstock_line(df, ts_codes, y_col='pct_chg', legend_labels=None, datespan=None, colors=None, w=800, h=450, verbose=False):
    summary = '''
        绘制多支股票的区间累积涨跌幅线
    '''
    if verbose:
        print(summary)
    p = figure(plot_width=w, plot_height=h,
               title = 'Multi-line Plot',
               x_axis_label = '交易日期', y_axis_label = '累积涨幅')
    p.xaxis[0].formatter = DatetimeTickFormatter()

    colors = inferno(len(ts_codes)) if colors is None else colors
    legend_it=[]

    for ts_code, color in zip(ts_codes, colors):
        tmpdf = df.loc[ts_code].copy()
        if datespan is not None:
            tmpdf=tmpdf[datespan]
        if y_col == 'pct_chg':
            c = p.line(tmpdf.index, tmpdf.pct_chg.cumsum(), line_width=2, color=color)
        else:
            c = p.line(tmpdf.index, tmpdf[y_col], line_width=2, color=color)
        legend = ts_code if legend_labels is None else legend_labels.loc[ts_code]
        legend_it.append((legend, [c]))

    legend = Legend(items=legend_it, location=(0, -60), click_policy='mute')
    p.add_layout(legend, 'right')
    show(p)


