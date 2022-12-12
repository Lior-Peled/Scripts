import sys
sys.path.insert(0, "/opt/bitnami/airflow/dags/git_dags/")

from sqlalchemy.orm import sessionmaker
import pandas as pd
from datetime import datetime
from utils.common import get_logger, get_db_schema, get_db
from constants import BI_DB_NAME


logger = get_logger(__name__)


def main():
    engine_bi = get_db(BI_DB_NAME)
    schema = get_db_schema()
    sp_list = [
        f""" {schema}."SP_STG_a"() """,
        f""" {schema}."SP_STG_b"() """
      
    ]
    execute_stored_procedures(schema, engine_bi, sp_list)


def execute_stored_procedures(schema, engine, sp_list):
    session_maker = sessionmaker(bind=engine)
    conn = engine.connect()
    for row in sp_list:
        session = session_maker()
        try:
            session.execute(f"""call {row} """)
            session.commit()
            result = pd.DataFrame({'StoredProcedureName': [f"{row}"], 'ExecutionStatus': ['Success'],
                                   'ExecutionDateTime': [datetime.today()]})
            result.to_sql("MNG_", engine, schema="DEV", if_exists='append', index=False)
            logger.info(f"{row} Commit")
        except:
            session.rollback()
            result = pd.DataFrame({'StoredProcedureName': [f"{row}"], 'ExecutionStatus': ['Failed'],
                                   'ExecutionDateTime': [datetime.today()]})
            result.to_sql("MNG_", engine, schema="DEV", if_exists='append', index=False)
            logger.info(f"{row} Failed")
            raise
    conn.close()


if __name__ == "__main__":
    main()
