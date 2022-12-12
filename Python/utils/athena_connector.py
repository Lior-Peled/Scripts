import sys

sys.path.insert(0, "/opt/bitnami/airflow/dags/git_dags/")

from pyathena import connect, connection
from utils.athena_connection_config import AthenaConnectionConfig


class AthenaConnector:
    @staticmethod
    def get_engine(config: AthenaConnectionConfig) -> connection:
        return AthenaConnector._create_engine(config)

    @staticmethod
    def _create_engine(config: AthenaConnectionConfig) -> connection:
        return connect(
                aws_access_key_id=f'{config.access_key}',
                aws_secret_access_key=f'{config.secret_key}',
                s3_staging_dir=f'{config.s3_staging_dir}',
                region_name=f'{config.region_name}').cursor()
