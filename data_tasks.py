import sys
sys.path.append('./')


from env import *
from sqlbase import engine, db_session, text, desc

from utils.datetimes import daterange, trade_day_util
from utils.argparser import data_params_wrapper
from utils.stock_utils import *
from utils.psql_client import load_table, insert_df, get_stock_basic
from utils.datasource import ak, ak_all_plates, ak_today_auctions, ak_activity, ak_today_index, ak_get_limit, ak_daily_prices, ak_stock_basics, ak_daily_basic
from utils.datetimes import biquater_ago_date, end_date as tdu_end_date, today_date as tdu_today_date, trade_day_util as tdu
from utils.logger import logger, logging

from models import *
from data_center import DataCenter, init_data


DAILY_MODELS = [StockBasic, DailyBasic, Price, AdjFactor, Money, LimitStock]
DATE_MODELS = []

import paramiko
logging.getLogger("paramiko").setLevel(logging.INFO)


def get_connection(hostname):
    s = paramiko.SSHClient()
    s.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    s.connect(hostname, key_filename='/Users/tzhu/.ssh/id_rsa')
    return s


def get_table_columns(modelname):
    q=db_session.query(modelname).limit(1).statement
    columns = list(engine.execute(q)._metadata.keys)
    columns.remove('id')
    if 'limit' in columns:
        columns.remove('limit')
        columns.append('\\"limit\\"')
    return ', '.join(columns)


def get_file_from_remote(host, remote_file, local_file):
    s = get_connection(host)
    ftp_client = s.open_sftp()
    ftp_client.get(remote_file, local_file)
    ftp_client.close()


def load_table_from_host(model, hostname, trade_date):
    s = get_connection(hostname)
    filename = f'{model.__tablename__}_{trade_date}.csv'
    copy_command = f'psql -d stock -c "copy (select {get_table_columns(model)} from {model.__tablename__} where trade_date=\'{trade_date}\') to \'/tmp/{filename}\' delimiter \'|\' CSV header"'
    logger.debug(copy_command)
    stdin, stdout, stderr = s.exec_command(copy_command)
    logger.debug(stdout)
    if len(stderr.readlines()) == 0: # success
        localfile = f'{os.getcwd()}/tmp/{filename}'
        ftp_client = s.open_sftp()
        ftp_client.get(f'/tmp/{filename}', f'{localfile}')
        ftp_client.close()
        t = pd.read_csv(localfile, delimiter='|')
        logger.info(f'{trade_date} Got {len(t)} {model.__tablename__} data from {hostname}, loading...')
        insert_df(df=t, tablename=model.__tablename__)
        logger.info(f'{trade_date} {model.__tablename__} local file {localfile} is loaded.')
        return t
    else:
        logger.error(stderr)
        return None


@data_params_wrapper
def fetch_ak_auctions():
    summary = """
    # 接口：AkShare auction
    # only for last trading day
    """
    ts_codes = get_stock_basic(tdu_end_date).index
    ak_today_auctions(ts_codes, save_db=True)


@data_params_wrapper
def fetch_indices():
    summary = """
    # 接口：AkShare index
    # only for last trading day, run through 15:30 to 23:59
    """
    today_date = tdu.past_trade_days(tdu_today_date, days=1)[-1]
    stock_zh_index_spot_df = ak_today_index()
    stock_zh_index_spot_df.loc[:, 'trade_date'] = today_date
    insert_df(stock_zh_index_spot_df, 'indices')
    logger.info(f"{today_date} Index Data is updated. Total: {len(stock_zh_index_spot_df)}")


@data_params_wrapper
def fetch_stock_basics():
    """
    接口：stock_basic
    描述：获取基础信息数据，包括股票代码、名称、上市日期、退市日期等
    """
    stk_basic = ak_stock_basics(savedb=True)
    logger.info(f"StockBasic is updated. Total: {len(stk_basic)}")


@data_params_wrapper
def fetch_daily_basic(start_date, end_date):
    """
    接口：THS
    描述：Get free share, free mv, ma_close_250, etc
    """
    logger.info('fwefwe')
    for trade_date in tdu.trade_days_between(start_date, end_date):
        start_at = pdl.now()
        logger.info(f"Fetching {trade_date} Daily Basic from THS...")
        dbasic = ak_daily_basic(trade_date)
        insert_df(dbasic.drop(columns='name').reset_index(), tablename=DailyBasic.__tablename__)
        logger.info(f'\nGot {len(dbasic)} Stock Basic Records. Spent time: {(pdl.now()-start_at).in_seconds()} seconds.')


@data_params_wrapper
def fetch_ak_prices(start_date, end_date):
    for trade_date in tdu.trade_days_between(start_date, end_date):
        logger.info("Getting prices of {}".format(trade_date))
        adj_factor, price, paused_stocks = ak_daily_prices(trade_date)
        logger.info(f"Saving AdjFactor of {trade_date}, total count: {len(adj_factor)}")
        insert_df(adj_factor.reset_index(), AdjFactor.__tablename__)
        logger.info(f"Saving Price of {trade_date}, total count: {len(price)}")
        price_df = price.reset_index()[['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount']]
        insert_df(df=price_df, tablename=Price.__tablename__)
        logger.info(f"Price data saved({len(paused_stocks)} stocks are not trading today).")


@data_params_wrapper
def fetch_ak_limits(start_date, end_date):
    for trade_date in tdu.trade_days_between(start_date, end_date):
        logger.info("Getting limits of {}".format(trade_date))
        upstops = ak_get_limit(trade_date, savedb=True)
        logger.info(f'[{trade_date}] Got {len(upstops[upstops.limit=="U"])}/{len(upstops[upstops.limit=="D"])}/{len(upstops[upstops.limit=="Z"])} (U/D/Z) upstops.')


@data_params_wrapper
def show_data_status():
    for model in DAILY_MODELS:
        if hasattr(model, 'trade_date'):
            date_attr = 'trade_date'
            start = db_session.query(model).order_by('trade_date').first()
            end = db_session.query(model).order_by(text('trade_date desc')).first()
        else:
            date_attr = 'date'
            start = db_session.query(model).order_by('date').first()
            end = db_session.query(model).order_by(text('date desc')).first()

        if start:
            print(f'{model.__name__} starts from {getattr(start, date_attr)}')
            print(f'{model.__name__} ends to {getattr(end, date_attr)}')
            print('')
        else:
            print('No {model.__name__} records.')



@data_params_wrapper
def pull_data(start_date=None, end_date=None):
    hostname = 'elon'
    from neo import data_tasks
    if end_date is None:
        from utils.datetimes import end_date as last_trade_date
        end_date = last_trade_date
    if start_date is None:
        start_date = trade_day_util.past_trade_days(days=5)[0]
    days_to_check = trade_day_util.trade_days_between(start_date=start_date, end_date=end_date)

    logger.info(f'Checking days from {days_to_check[0]} to {days_to_check[-1]}, {len(days_to_check)} days in total.')

    logger.info('Update StockBasic anyway...')
    fetch_stock_basics()

    # 最近的一个交易日
    today_date = tdu.past_trade_days(tdu_today_date, days=1)[-1]

    for day in tqdm(days_to_check):
        logger.info(f'Checking Price/AdjFactor of {day}...')
        adj_factor = load_table(AdjFactor, start_date=day, end_date=day)
        price = load_table(Price, start_date=day, end_date=day)
        min_count = min(len(adj_factor), len(price))
        if min_count < expected_count_in_day(Price, day):
            load_table_from_host(AdjFactor, hostname, day)
            load_table_from_host(Price, hostname, day)

        logger.info(f'Checking DailyBasic of {day}...')
        dbasic = load_table(DailyBasic, start_date=day, end_date=day)
        if len(dbasic) < expected_count_in_day(DailyBasic, day):
            load_table_from_host(DailyBasic, hostname, day)

        logger.info(f'Checking Upstops of {day}...')
        upstops = load_table(LimitStock, start_date=day, end_date=day)
        logger.info(f"[{day}]. LimitStock count: {len(upstops)}, Expected: {expected_count_in_day(LimitStock, day)}.")
        if len(upstops) < expected_count_in_day(LimitStock, day):
            load_table_from_host(LimitStock, hostname, day)

        logger.info(f"Checking {day} Auction data...")
        df = load_table(Auction, start_date=day, end_date=day)
        logger.info(f"[{day}]. Auction count: {len(df)}, Expected: {expected_count_in_day(Auction, day)}.")
        if len(df) < expected_count_in_day(Auction, day):
            load_table_from_host(Auction, hostname, day)
            logger.info(f"[{day}] Auction Fixed. You might want to re-run this check after all done.")

        logger.info(f"Checking {day} Index data...")
        df = load_table(Index, start_date=day, end_date=day)
        logger.info(f"[{day}]. Index count: {len(df)}, Expected: {expected_count_in_day(Index, day)}.")
        if len(df) < expected_count_in_day(Index, day):
            load_table_from_host(Index, hostname, day)

        logger.info(f'Get emo of {day}')
        load_table_from_host(Activity, hostname, day)

    logger.info("Pulling plates data...")
    local_plate_file = './data/plates.feather'
    plates = pd.read_feather(local_plate_file)
    logger.info(f'[Before] Plate file has {len(plates)} records.')
    get_file_from_remote(hostname, '/var/neo_world/codebase/data/plates.feather', local_plate_file)
    plates = pd.read_feather(local_plate_file)
    logger.info(f'[After] Plate file has {len(plates)} records.')

    return None


@data_params_wrapper
def check_data_integrity(start_date=None, end_date=None, try_fix=True, sleep_time=2, from_elon=False):
    from neo import data_tasks
    if end_date is None:
        from utils.datetimes import end_date as last_trade_date
        end_date = last_trade_date
    if start_date is None:
        start_date = trade_day_util.past_trade_days(days=30)[0]
    days_to_check = trade_day_util.trade_days_between(start_date=start_date, end_date=end_date)

    logger.info(f'Checking days from {days_to_check[0]} to {days_to_check[-1]}, {len(days_to_check)} days in total.')

    logger.info('Update StockBasic anyway...')
    fetch_stock_basics()

    # 最近的一个交易日
    today_date = tdu.past_trade_days(tdu_today_date, days=1)[-1]

    for day in tqdm(days_to_check):
        logger.info(f'Checking Price/AdjFactor of {day}...')
        adj_factor = load_table(AdjFactor, start_date=day, end_date=day)
        price = load_table(Price, start_date=day, end_date=day)
        min_count = min(len(adj_factor), len(price))
        if min_count < expected_count_in_day(Price, day):
            logger.warn(f"[{day}]. Price {len(price)}, AdjFactor {len(adj_factor)}.")
            data_tasks('price')(start_date=day, end_date=day)
            logger.info(f"[{day}] Price Fixed. You might want to re-run this check after all done.")

        logger.info(f'Checking DailyBasic of {day}...')
        dbasic = load_table(DailyBasic, start_date=day, end_date=day)
        if len(dbasic) < expected_count_in_day(DailyBasic, day):
            logger.warn(f"[{day}]. DailyBasic {len(dbasic)}.")
            data_tasks('daily_basic')(start_date=day, end_date=day)
            logger.info(f"[{day}] DailyBasic Fixed. You might want to re-run this check after all done.")

        logger.info(f'Checking Upstops of {day}...')
        upstops = load_table(LimitStock, start_date=day, end_date=day)
        if len(upstops) < expected_count_in_day(LimitStock, day):
            logger.warn(f"[{day}]. LimitStock {len(upstops)}.")
            data_tasks('upstop')(start_date=day, end_date=day)
            logger.info(f"[{day}] LimitStock Fixed. You might want to re-run this check after all done.")

        # Below tables only fix recent day
        logger.info(f"Checking {day} Auction data...")
        df = load_table(Auction, start_date=day, end_date=day)
        logger.info(f"[{day}]. Auction count: {len(df)}, Expected: {expected_count_in_day(Auction, day)}.")
        if len(df) < expected_count_in_day(Auction, day):
            if day == today_date:
                data_tasks('auction')()
                logger.info(f"[{day}] Auction Fixed. You might want to re-run this check after all done.")

        logger.info(f"Checking {day} Index data...")
        df = load_table(Index, start_date=day, end_date=day)
        logger.info(f"[{day}]. Index count: {len(df)}, Expected: {expected_count_in_day(Index, day)}.")
        if len(df) < expected_count_in_day(Index, day):
            if day == today_date:
                data_tasks('index')()
                logger.info(f"[{day}] Index Data Fixed. You might want to re-run this check after all done.")

    logger.info('Get emo')
    ak_activity(save=True)

    return None


def expected_count_in_day(model, date):
    if date > '2022-05-01':
        base_count = 4550
    if date > '2021-11-11':
        base_count = 4400
    if date > '2020-12-01':
        base_count = 4000
    elif date > '2020-01-01':
        base_count = 3700
    elif date > '2019-01-01':
        base_count = 3400
    elif date > '2018-01-01':
        base_count = 3100
    else:
        base_count = 1600

    if model in [LimitStock, UpStop, Dragon]:
        return 30
    elif model in [Index]:
        return 400
    elif model in [Money, Auction]:
        from utils.datetimes import week_ago_date
        return len(load_table(Price, week_ago_date, week_ago_date)) - 160
    else:
        return base_count


def bulkload_csv_data_to_database(engine, tablename, columns, data, sep=","):
    logging.info("Start ingesting data into postgres ...")
    logging.info("Table name: {table}".format(table=tablename))
    logging.info("CSV schema: {schema}".format(schema=columns))
    conn = engine.connect().connection
    cursor = conn.cursor()
    cursor.copy_from(data, tablename, columns=columns, sep=sep, null='null')
    conn.commit()
    conn.close()
    logging.info("Finish ingesting")


# samples 

# news
# df = pro.cctv_news(date='20181211')
# df = pro.news(src='sina', start_date='20181121', end_date='20181122')
# 获取单个股票公告数据
# df = pro.anns(ts_code='002149.SZ', start_date='20190401', end_date='20190509', year='2019')
# 获取2019年最新的50条公告数据
# df = pro.anns(year='2019')

