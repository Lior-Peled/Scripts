import pandas as pd
from dynaconf import settings
from utils.common import get_logger, get_db
from sqlalchemy.orm import sessionmaker
import os

ENV = settings.get("environment")

logger = get_logger(__name__)

RESOURCE_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "dwh_scripts")


def build_db(engine):
    session_maker = sessionmaker(bind=engine)
    conn = engine.connect()
    build_schema(engine)
    for folder in os.walk(RESOURCE_DIR):
        session = session_maker()
        folder_list = pd.Series(folder[1])
        folder_list = folder_list.sort_values()
        for row in folder_list:
            if '_scripts' in row:
                path = f"{folder[0]}/{row}"
                files_list = os.walk(path)
                for file in files_list:
                    file_name = pd.Series(file[2])
                    for file in file_name:
                        try:
                            sql_file = open(f"{path}/{file}")
                            sql_as_string = sql_file.read()
                            session.execute(sql_as_string)
                            session.commit()
                        except:
                            session.rollback()
                            logger.info(f"filed to commit sql file - {file}")
    conn.close()
    logger.info(f"commit  db schema")


def build_schema(engine):
    session_maker = sessionmaker(bind=engine)
    conn = engine.connect()
    session = session_maker()
    session.execute('DROP schema IF exists "DEV" CASCADE;')
    session.commit()
    session.execute('CREATE SCHEMA "DEV" AUTHORIZATION admin;')
    session.commit()
    conn.close()


if __name__ == "__main__":
    build_db()
