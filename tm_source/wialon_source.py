import requests
import json

from tm_source.abs_transport_src import AbstractTransportSource


class WialonSource(AbstractTransportSource):

    def __init__(self, login = None, client_id = None, secret_key = None, password = None, access = None):
        super().__init__(login, client_id, secret_key, password)
        self.refresh_token: str | None = secret_key
        self.access_token: str | None = None
        self.token_type: str | None = None
        self.user_id: str | None = None
        self.BASE_URL: str = 'https://hst-api.wialon.com/wialon/ajax.html?svc='

    def get_velocity_zones(self):
        pass

    def get_planned_routes(self):
        pass

    def get_incidents(self):
        pass

    def get_transport_list(self):
        if not self.is_connected():
            self.auth()
        params = {
            "spec": {
                "itemsType": "avl_unit",
                "propName": "avl_unit",
                "propValueMask": "*",
                "sortType": "sys_name",
                "propType": "avl_unit"
            },
            "force": 1,
            "flags": 8388609,
            "from": 0,
            "to": 0
        }
        res = requests.get(self.BASE_URL + 'core/search_items&params=' + self.convert_params(params) + f"&sid={self.access_token}")
        if res.status_code != 200:
            return False
        return res.json()

    def get_transports(self, query_filter: str = ''):
        if not self.is_connected():
            self.auth()
        params = {
            "spec": {
                "itemsType": "avl_unit",
                "propName": "avl_unit",
                "propValueMask": "*",
                "sortType": "sys_name",
                "propType": "avl_unit"
            },
            "force": 1,
            "flags": 11535361,
            "from": 0,
            "to": 0
        }
        res = requests.get(self.BASE_URL + 'core/search_items&params=' + self.convert_params(params) + f"&sid={self.access_token}")
        if res.status_code != 200:
            return False
        return res.json()

    def is_connected(self):
        if not self.access_token:
            return False
        return True


    def auth(self):
        params = {"token": self.refresh_token}
        res = requests.get(self.BASE_URL + 'token/login&params=' + json.dumps(params))
        if res.status_code != 200:
            return False
        self.update_token(res.json())
        return True

    def update_token(self, token_params: dict):
        self.access_token = token_params['eid']
        print(self.access_token)

    @staticmethod
    def convert_params(params):
        return json.dumps(params)
