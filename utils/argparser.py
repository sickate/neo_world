import pendulum as pdl
import argparse
from functools import wraps

def model_arg_parser():
    parser = argparse.ArgumentParser(description='model arguments.')
    parser.add_argument("-t", "--ts-code", dest="ts_code", default=None)
    parser.add_argument("-s", "--start-date", dest="start_date", default='2016-01-01')
    parser.add_argument("-e", "--end-date", dest="end_date", default=pdl.now().to_date_string())
    parser.add_argument("-v", "--verbose", action="store_true", dest="verbose", default=False)
    parser.add_argument("-m", "--mode", action="store_true", dest="mode", default='local')
    options = parser.parse_args()
    return options


def daily_job_options():
    parser = argparse.ArgumentParser(description='Daily data job arguments.')
    parser.add_argument("-s", "--start-date", dest="start_date")
    parser.add_argument("-e", "--end-date", dest="end_date")
    parser.add_argument("-r", "--remote", dest="remote", action='store_true', default=False)
    parser.add_argument("-d", "--save_db", dest="save_db", action='store_true', default=False)
    parser.add_argument("-v", "--verbose", dest="verbose", action='store_true', default=False)
    parser.add_argument("-n", "--notification", dest="send_notification", action='store_true', default=False)
    options = parser.parse_args()
    return options



def data_task_options(task_name_desc):
    parser = argparse.ArgumentParser(description='Data task arguments.')
    parser.add_argument('task_type', help='task_type: db, data, help')
    parser.add_argument('task_name', help=task_name_desc)
    parser.add_argument("-s", "--start-date", dest="start_date")
    parser.add_argument("-e", "--end-date", dest="end_date")
    parser.add_argument("-m", "--model_name", dest="model_name")
    parser.add_argument("--ts_code")
    parser.add_argument("--trade_date")
    parser.add_argument("-v", "--verbose", dest="verbose", action='store_true')
    options = parser.parse_args()
    return options


def realtime_options():
    parser = argparse.ArgumentParser(description='model arguments.')
    parser.add_argument("-d", "--date", dest="batch_date")
    parser.add_argument("-t", "--task-name", dest="task_name")
    parser.add_argument("-m", "--should-move", dest="should_move", default=False)
    options = parser.parse_args()
    return options


# filter empty (optional) arguments passed from cmd line
def data_params_wrapper(func):
    @wraps(func)
    def filter_none_args(**kwargs):
        new_kwargs = {}
        for kw in kwargs.keys():
            if kw == 'verbose' and kwargs[kw] != True:
                continue
            if kwargs[kw] != None:
                new_kwargs[kw] = kwargs[kw]
        # print(new_kwargs)
        return func(**new_kwargs)
    return filter_none_args
