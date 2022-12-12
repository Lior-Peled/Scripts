import sys

sys.path.insert(0, "/opt/bitnami/airflow/dags/git_dags/")

from utils.common import get_logger, get_db_schema, get_db
from utils.execution import execute_step
from constants import DWH_TABLES_TO_RUN, BI_DB_NAME

logger = get_logger(__name__)


def execute_dwh_step():
    engine_bi = get_db(BI_DB_NAME)
    schema = get_db_schema()
    execute_step(engine_bi, schema, tables_to_run=DWH_TABLES_TO_RUN, step_type='dwh')


if __name__ == "__main__":
    execute_dwh_step()

