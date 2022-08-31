# Entry point
# Author: tuo

from sqlbase import *
from db_tasks import *
from data_tasks import *
from realtime_tasks import *
from utils.argparser import data_task_options

task_name_desc = """
    db tasks:
        - init
        - reset
        - drop [model_name]
        - create [model_name]
        // not implemented
        - export table_name
        - trunc table_name
        - seed [table_name]
        - refresh [table_name]

    data tasks:
        - prices
        - daily_basic
        """


def db_tasks(task):
    funcs = {
            'init' : init_db,
            'reset' : reset_db,
            'drop' : drop_tables,
            'create' : create_tables
            }
    return funcs.get(task, method_not_exist)


def data_tasks(task):
    funcs = {
            'auction': fetch_ak_auctions,
            'price': fetch_ak_prices,
            'upstop': fetch_ak_limits,
            'stock_basic': fetch_stock_basics,
            'index': fetch_indices,
            'daily_basic': fetch_daily_basic,
            # 'money': fetch_money,

            'info': show_data_status,
            'check': check_data_integrity,
        }
    return funcs.get(task, method_not_exist)


def realtime_tasks(task):
    funcs = {
            'monitor': monitor,
    }
    return funcs.get(task, method_not_exist)


def record_tasks(task):
    funcs = {
            'order': insert_order,
            }
    return funcs.get(task, method_not_exist)


def method_not_exist():
    print('Task definition not exist.')
    pass


if __name__ == '__main__':
    # drop_tables()
    # parser = argparse.ArgumentParser(description='Tasks')
    # parser.add_argument('task_type', help='task_type: db, data, help')
    # parser.add_argument('task_name', help=task_name_desc)

    # data task arguments
    # parser.add_argument("--ts_code")
    # parser.add_argument("--trade_date")
    # parser.add_argument("--start_date")
    # parser.add_argument("--end_date")

    # db task arguments
    # parser.add_argument("--model_name")
    # options = parser.parse_args()

    options = data_task_options(task_name_desc)

    print(options)
    if options.task_type == 'db':
        db_tasks(options.task_name)(
            model_name=options.model_name
        )
    elif options.task_type == 'data':
        data_tasks(options.task_name)(
            start_date=options.start_date,
            end_date=options.end_date,
            trade_date=options.trade_date,
            ts_code=options.ts_code,
            verbose=options.verbose)
    elif options.task_type == 'rt':
        realtime_tasks(options.task_name)()
    elif options.task_type == 'rec':
        record_tasks(options.task_name)(
            start_date=options.start_date,
            end_date=options.end_date,
            trade_date=options.trade_date,
            ts_code=options.ts_code,
            verbose=options.verbose)


    else:
        print('Not implemented. Yet.')


# one way
# df.to_sql('table_name', engine)

# faster
# df.head(0).to_sql('table_name', engine, if_exists='replace',index=False) #truncates the table

# conn = engine.raw_connection()
# cur = conn.cursor()
# output = io.StringIO()
# df.to_csv(output, sep='\t', header=False, index=False)
# output.seek(0)
# contents = output.getvalue()
# cur.copy_from(output, 'table_name', null="")
# conn.commit()
