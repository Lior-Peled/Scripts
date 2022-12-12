from dynaconf import settings
import requests


class PBIConnector:
    @staticmethod
    def get_auth_token(conf):
        PBI = settings.get(conf)
        POWER_BI_RESOURCE_ENDPOINT = PBI.POWER_BI_RESOURCE_ENDPOINT

        power_bi_username = PBI.power_bi_username
        power_bi_password = PBI.power_bi_password
        power_bi_client_id = PBI.power_bi_client_id

        URL = "https://login.windows.net/common/oauth2/token/"
        BODY = {"resource": f"{POWER_BI_RESOURCE_ENDPOINT}",
                "client_id": f"{power_bi_client_id}",
                "grant_type": "password",
                "username": f"{power_bi_username}",
                "password": f"{power_bi_password}",
                "scope": "openid"}
        HEADERS = {'Content-Type': 'application/x-www-form-urlencoded'}
        return requests.post(url=URL, data=BODY, headers=HEADERS).json()['access_token']
