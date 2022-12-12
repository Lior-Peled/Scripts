import sys

sys.path.insert(0, "/opt/airflow/dags/git_dags/")

import collections
import os
import subprocess
import logging
import logging.config
import pandas as pd
from dynaconf import settings
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from utils.db_connection_config import DBConnectionConfig
from utils.db_connector import DBConnector
from datetime import datetime

ENV = settings.get("environment")


def get_logger(name):
    # create logger for prd_ci
    log = logging.getLogger(name)
    log.setLevel(level=logging.INFO)

    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(filename)s - %(message)s')

    # create console handler for logger.
    ch = logging.StreamHandler()
    ch.setLevel(level=logging.INFO)
    ch.setFormatter(formatter)

    # add handlers to logger.
    log.addHandler(ch)
    return log


def flatten_and_lower(d, parent_key="", sep="_"):
    items = []
    for k, v in d.items():
        k = k.lower()
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.abc.MutableMapping):
            items.extend(flatten_and_lower(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)



def get_db_schema():
    if ENV == "Dev":
        schema = 'BI_DWH_DEV'
    else:
        schema = 'BI_DWH'
    return schema


def create_engines():
    dwh_conf = settings.get("bi_dwh")
    bi_dwh_engine = create_engine(
        f"postgresql://{dwh_conf.user}:{dwh_conf.password}@{dwh_conf.host}:{dwh_conf.port}/{dwh_conf.db}")
    return bi_dwh_engine


def get_db(dynaconf_settings_name: str) -> Engine:
    dynaconf_config = settings.get(dynaconf_settings_name)
    config = DBConnectionConfig.from_dict(dynaconf_config)
    engine = DBConnector.get_engine(config)
    return engine


def execute_query(query, source_db):
    engine_bi = get_db(source_db)
    trunc_exe = pd.read_sql_query(query, con=engine_bi)
    conn = engine_bi.connect()
    for row in trunc_exe["query"].values:
        conn.execute(row)
    conn.close()


def execute_singal_query(query, source_db):
    engine_bi = get_db(source_db)
    with engine_bi.connect() as conn:
        conn.execute(query)
    conn.close()


def execute_stored_procedures(schema, engine, sp_list, logger):
    session_maker = sessionmaker(bind=engine)
    conn = engine.connect()
    for row in sp_list:
        session = session_maker()
        try:
            session.execute(f"""call {row} """)
            session.commit()
            result = pd.DataFrame({'StoredProcedureName': [f"{row}"], 'ExecutionStatus': ['Success'],
                                   'ExecutionDateTime': [datetime.today()]})
            result.to_sql("MNG_TABLE", engine, schema='DEV', if_exists='append', index=False)
            logger.info(f"{row} Commit")
        except:
            session.rollback()
            result = pd.DataFrame({'StoredProcedureName': [f"{row}"], 'ExecutionStatus': ['Failed'],
                                   'ExecutionDateTime': [datetime.today()]})
            result.to_sql("MNG_TABLE", engine, schema='DEV', if_exists='append', index=False)
            logger.info(f"{row} Failed")
            raise
    conn.close()
