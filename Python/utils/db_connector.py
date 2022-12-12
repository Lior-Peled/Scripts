import sys

sys.path.insert(0, "/opt/airflow/dags/git_dags/")

import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from utils.db_connection_config import DBConnectionConfig


class DBConnector:
    @staticmethod
    def get_engine(config: DBConnectionConfig) -> Engine:
        connection_string = DBConnector.__get_connection_string(config)
        return DBConnector._create_engine(connection_string)

    @staticmethod
    def _create_engine(connection_string: str) -> Engine:
        return create_engine(connection_string)

    @staticmethod
    def __get_connection_string(config: DBConnectionConfig) -> str:
        return f"postgresql://{config.user}:{config.password}@{config.host}:{config.port}/{config.db}"

    @staticmethod
    def execute_query(query: str, engine: Engine) -> DataFrame:
        return pd.read_sql_query(query, con=engine)

    @staticmethod
    def get_lookup_date_value(table_name: str, engine: Engine) -> DataFrame:
        query = """ select COALESCE(max(r."LastUpdatedValue"),current_date)  - (30 * interval '1 min') as date 
        from "dbo"."MNG_Incremental" r where r."TableName" = '{table_name}' """. format(table_name=table_name)
        return pd.read_sql_query(query, con=engine)

    @staticmethod
    def write_to_db(table_name, df, engine, write_type, schema='dbo'):
        df.to_sql(table_name, engine, schema, if_exists=write_type, index=False)
