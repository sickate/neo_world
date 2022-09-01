"""PostgreSQL Loader & Writers

Including multiple useful helper methods.

Todo:
    * placeholders
"""

import sys
sys.path.append('./')

from env import *
from models import *
from sqlbase import engine, db_session, build_sql, text, desc, StringIO
from sqlalchemy.exc import IntegrityError

from utils.datetimes import *
from utils.logger import logger

# 多进程模式，要传链接字符串而非对象
if 'modin' in pd.__path__[0]:
    logger.info('Using Modin')
    from sqlbase import conn_str as engine
else:
    from sqlbase import engine


#############################
# Load data from db
#############################

def get_stock_basic(tdate=None, basic_filter=False):
    """
        从 pg db 获取截止当日的股票基本信息
    """
    tdate = end_date if tdate is None else tdate
    stocks = pd.read_sql_table('stock_basic', engine)
    stocks = stocks[stocks.list_date <= tdate.replace('-','')]
    return stocks.set_index('ts_code')


def load_stock_prices(start_date, end_date, ts_codes=None, fast_load=True):
    """
        从 pg db 获取一批股票在一段时内的数据, 包括：Price, DailyBasic, AdjFactor
        配合 gen_adj_price 对价格进行除权
    """
    # Get Price
    query_price = (
        db_session.query(Price)
            .filter(Price.trade_date >= start_date)
            .filter(Price.trade_date <= end_date)
            .order_by(text('trade_date asc'))
    )
    if ts_codes is not None:
        query_price = query_price.filter(Price.ts_code.in_(ts_codes))

    logger.info(f'Start loading price data from PG...')
    if fast_load:
        price = read_pg(query=query_price)
    else:
        price = pd.read_sql(query_price.statement, engine)
    price['trade_date'] = pd.to_datetime(price.trade_date)
    price.set_index(['ts_code', 'trade_date'], inplace=True)

    # Get AdjFactor
    query_adjfactor = (
        db_session.query(AdjFactor)
            .filter(AdjFactor.trade_date >= start_date)
            .filter(AdjFactor.trade_date <= end_date)
    )
    if ts_codes is not None:
        query_adjfactor = query_adjfactor.filter(AdjFactor.ts_code.in_(ts_codes))

    logger.info(f'Start loading adjfactor data from PG...')
    if fast_load:
        adjfactor = read_pg(query=query_adjfactor)
    else:
        adjfactor = pd.read_sql(query_adjfactor.statement, engine)
    adjfactor['trade_date'] = pd.to_datetime(adjfactor.trade_date)
    adjfactor.set_index(['ts_code', 'trade_date'], inplace=True)

    # Get DailyBasic
    query_basic = (
        db_session.query(DailyBasic)
            .filter(DailyBasic.trade_date >= start_date)
            .filter(DailyBasic.trade_date <= end_date)
    )
    if ts_codes is not None:
        query_basic = query_basic.filter(DailyBasic.ts_code.in_(ts_codes))
    logger.info(f'Start loading daily_basic data from PG...')
    if fast_load:
        basic = read_pg(query=query_basic)
    else:
        basic = pd.read_sql(query_basic.statement, engine)
    basic['trade_date'] = pd.to_datetime(basic.trade_date)
    basic.rename(columns={'volume_ratio': 'vol_ratio'}, inplace=True)
    basic.set_index(['ts_code', 'trade_date'], inplace=True)

    logger.info(f'Merging...')
    df = price.join(adjfactor[['adj_factor']]).join(basic[['turnover_rate', 'turnover_rate_f',
        'vol_ratio', 'free_mv', 'total_share', 'float_share', 'free_share', 'ma_close_250']])
    return df.drop(columns='id')


def load_table(model, start_date, end_date, ts_codes=None, **kwargs):
    query= (
        db_session.query(model)
            .filter(model.trade_date >= start_date)
            .filter(model.trade_date <= end_date)
    )
    for k,v in kwargs.items():
        query = query.filter(getattr(model, k) == v)

    if ts_codes is not None:
        query = query.filter(getattr(model, 'ts_code').in_(ts_codes))
    if 'modin' in pd.__path__:
        logger.info(f'Loading table {model.__table__} with Modin..')
        sql = f'select * from {model.__table__} where trade_date >= \'{start_date}\' and trade_date <= \'{end_date}\''
        df = pd.read_sql(sql, engine)
    else:
        df = read_pg(query=query)
    df['trade_date'] = pd.to_datetime(df.trade_date)
    if 'ts_code' in df.columns:
        df.set_index(['ts_code', 'trade_date'], inplace=True)
    elif 'name' in df.columns:
        df.set_index(['name', 'trade_date'], inplace=True)
    else:
        df.set_index(['trade_date'], inplace=True)
    df = df.drop('id', axis=1)
    return df


##########################################################
# PSQL basic functions
##########################################################

# wrapper so we can switch inbetween modin/pd seeminglessly
def read_sql(statement):
    return pd.read_sql(statement, engine)


def insert_df(df, tablename):
    if df is None:
        return
    for i in range(len(df)):
        try:
            df.iloc[i:i+1].to_sql(tablename, con=engine, if_exists='append',
                            index=False, schema='public')
        except IntegrityError as e:
            pass


def to_pg(df, table_name, pg_engine):
    data = StringIO()
    df.to_csv(data, sep = '\t', header=False, index=False)
    data.seek(0)
    raw = pg_engine.raw_connection()
    cur = raw.cursor()
    cur.copy_from(data, table_name, sep = '\t')
    cur.connection.commit()
    cur.connection.close()
    cur.close()


def read_pg(pg_engine=None, table=None, sql=None, query=None):
    pg_engine = engine if pg_engine is None else pg_engine
    if sql is None:
        if table is not None:
            sql = f'select * from {table}'
        elif query is not None:
            sql = build_sql(query)
        else:
            return None
    head = 'HEADER'
    with tempfile.TemporaryFile() as tmpfile:
        copy_sql = f'COPY ({sql}) TO STDOUT WITH CSV {head}'
        conn = pg_engine.raw_connection()
        cur = conn.cursor()
        cur.copy_expert(copy_sql, tmpfile)
        tmpfile.seek(0)
        # df = pd.read_csv(tmpfile, index_col='id')
        df = pd.read_csv(tmpfile)
        return df


def get_record(object_id, class_name='Order'):
    klass = globals()[class_name]
    obj = klass.query.get(object_id)
    return obj


if __name__ == '__main__':

    from utils.argparser import model_arg_parser
    logger.info('System initializing...')

    options = model_arg_parser()
    logger.info(options)

    ts_codes = [options.ts_code] if options.ts_code else None
    start_date = options.start_date
    end_date = options.end_date
    verbose = options.verbose

    stock_basics = get_stock_basic(today_date=end_date)
    logger.info(f'Get {len(stock_basics)} stocks.')
    logger.info(stock_basics.head(5))

    prices = load_stock_prices(start_date=start_date, end_date=end_date, ts_codes=ts_codes)
    logger.info(prices.head(5))
