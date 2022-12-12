import sys
sys.path.insert(0, "/opt/bitnami/airflow/dags/git_dags/")
import urllib.request as urllib2
import requests
from dynaconf import settings
from utils.common import get_logger
from utils.pbi_authenticating import PBIConnector
from constants import PBI_WORKSPACES_DATASETS
import time
import json



POWER_BI_RESOURCE_ENDPOINT = PBI.POWER_BI_RESOURCE_ENDPOINT
MICROSOFT_OAUTH2_API_ENDPOINT = PBI.MICROSOFT_OAUTH2_API_ENDPOINT

sleep_seconds = 120


def main():
    general_error_flag = 0
    for key, value in PBI_WORKSPACES_DATASETS.items():

        auth_token = PBIConnector.get_auth_token(sys.argv[1])
        power_bi_dataset_list = [value] if isinstance(value, str) else value

        for ds in power_bi_dataset_list:
            single_error_flag, error, data_set_name = refresh_dataset(auth_token, key, ds)
            if single_error_flag ==0:
                logger.info(f"dataset {data_set_name} refreshed")
            else:
                logger.info(f"dataset {data_set_name} failed to refresh with error {error}")
            general_error_flag += single_error_flag
    assert general_error_flag == 0, f'refresh operation of PowerBi datasets failed'


def refresh_dataset(auth_token, _power_bi_group, _power_bi_dataset: str):
    try:
        URL = f"https://api.powerbi.com/v1.0/myorg/groups/{_power_bi_group}/datasets/{_power_bi_dataset}/refreshes"
        HEADERS = {'Content-Type': 'application/json',
                   'Authorization': f'Bearer {auth_token}'}
        r = requests.post(url=URL, headers=HEADERS)



        header = {'Authorization': f'Bearer {auth_token}'}

        # #get the dataset name
        dataset_url = f'https://api.powerbi.com/v1.0/myorg/groups/{_power_bi_group}/datasets/{_power_bi_dataset}'
        time.sleep(1)
        data_set_name = json.loads(requests.get(url=dataset_url, headers=header).content)['name']
        logger.info(f'request to refresh dataset {data_set_name} received back status code {r.status_code}')

        refresh_status_url = f'https://api.powerbi.com/v1.0/myorg/groups/{_power_bi_group}/datasets/{_power_bi_dataset}/refreshes?$top=1'

        time.sleep(1)
        r_status = json.loads(requests.get(url=refresh_status_url, headers=header).content)
        refresh_status = r_status['value'][0]['status']
        logger.info(f'refresh status is {refresh_status}')

        while refresh_status == 'Unknown':
            logger.info(f'status is {refresh_status}, retrying in {sleep_seconds} seconds')
            time.sleep(sleep_seconds)  # sleep 60 seconds
            r_status = json.loads(requests.get(url=refresh_status_url, headers=header).content)
            refresh_status = r_status['value'][0]['status']

        if refresh_status == 'Completed':
            return 0, None, data_set_name
        elif refresh_status == 'Failed':
            time.sleep(1)
            r_status = json.loads(requests.get(url=refresh_status_url, headers=header).content)
            failure_reason = r_status['value'][0]['serviceExceptionJson']
            logger.info(f"refresh operation on dataset {data_set_name} ended with status {refresh_status}")
            return 1, failure_reason, data_set_name

    except urllib2.HTTPError as e:
        status_code = e.code
        logger.info(status_code)

        return 1, 'general network error', data_set_name

    except json.decoder.JSONDecodeError as err:
        return 1, type(err), 'Unknown dataset'

if __name__ == "__main__":
    main()
