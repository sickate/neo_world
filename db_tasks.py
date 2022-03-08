from sqlbase import Base, engine, meta, db_session

from models import *
from utils.datetimes import daterange
from utils.argparser import data_params_wrapper

MODELS = [DailyBasic, Price, StockBasic, AdjFactor, Money, Order, UpStop]

@data_params_wrapper
def init_db():
    # this is not working with SQLAlchemyORM
    # figure it out later, if really neccessary
    # meta.create_all(engine)
    # meta.create_all(engine)
    create_tables()


@data_params_wrapper
def reset_db(model_name=None):
    """
    Drop database if exist. Create database afterwards.
    """

    if model_name:
        drop_tables(model_name=model_name)
        create_tables(model_name=model_name)
    else:
        drop_tables()
        create_tables()


@data_params_wrapper
def drop_tables(model_name=None):
    if model_name:
        if globals()[model_name]:
            globals()[model_name].__table__.drop(engine)
        else:
            print('Model name incorrect or not defined.')
    else:
        print("Drop db...")
        for model in MODELS:
            model.__table__.drop(engine)


@data_params_wrapper
def create_tables(model_name=None):
    if model_name:
        if globals()[model_name]:
            print(globals()[model_name].__table__)
            globals()[model_name].__table__.create(engine)
        else:
            print('Model name incorrect or not defined.')
    else:
        print("Create all db tables...")
        for model in MODELS:
            model.__table__.create(engine)
