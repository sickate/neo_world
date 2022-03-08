import tempfile
from io import StringIO

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, Date, UniqueConstraint, text, desc
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import IntegrityError

import os
from subprocess import check_output

current_user = check_output(['whoami']).decode().strip('\n')
host_name = check_output(['hostname']).decode().strip('\n')

conn_str_local = "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
            current_user,
            None,
            'localhost',
            5432,
            'stock_dev')

conn_str_remote = "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
            'tzhu',
            'mylifeforaiur',
            'alpha',
            5432,
            'stock')

conn_str = conn_str_local if current_user == 'tzhu' or current_user == 'daniel' else conn_str_remote

engine = create_engine(
            conn_str,
            client_encoding='utf8',
            isolation_level="READ UNCOMMITTED"
        )
meta = MetaData()
db_session = scoped_session(sessionmaker(autocommit=False,
    autoflush=False,
    bind=engine))

Base = declarative_base()
Base.query = db_session.query_property()

def build_sql(sqlalchemy_query):
    return str(sqlalchemy_query.statement.compile(
        dialect=postgresql.dialect(),
        compile_kwargs={"literal_binds": True}))

if __name__ == '__main__':
    df = read_pg('adj_factor', engine)
    to_pg(df, 'adj_factor', engine)



