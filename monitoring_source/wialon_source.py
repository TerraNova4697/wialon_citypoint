import requests
import json

from urllib3.exceptions import NameResolutionError

from monitoring_source.abs_transport_src import AbstractTransportSource
from monitoring_source.utils import report_error


class WialonSource(AbstractTransportSource):

    def __init__(self, login = None, client_id = None, secret_key = None, password = None):
        super().__init__(login, client_id, secret_key, password)
        self.refresh_token: str | None = secret_key
        self.access_token: str | None = None
        self.token_type: str | None = None
        self.user_id: str | None = None
        self.session = requests.session()
        self.BASE_URL: str = 'https://hst-api.wialon.com/wialon/ajax.html?svc='

    def get_velocity_zones(self):
        if not self.is_connected():
            self.auth()
        params = {
            "spec": {
                "itemsType": "avl_resource",
                "propName": "zones_library",
                "propValueMask": "*",
                "sortType": "sys_name",
                "propType": "propitemname"
            },
            "force": 1,
            "flags": 4097,
            "from": 0,
            "to": 0
        }
        try:
            res = self.session.get(
                self.BASE_URL + 'core/search_items&params=' + self.convert_params(params) + f"&sid={self.access_token}")
        except (requests.exceptions.ConnectionError, NameResolutionError, TimeoutError) as exception:
            self.session = requests.session()
            raise exception.__class__()
        if 200 <= res.status_code < 300:
            return res.json()
        report_error(res)

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
        try:
            res = self.session.get(self.BASE_URL + 'core/search_items&params=' + self.convert_params(params) + f"&sid={self.access_token}")
        except (requests.exceptions.ConnectionError, NameResolutionError, TimeoutError) as exception:
            self.session = requests.session()
            raise exception.__class__()
        if 200 <= res.status_code < 300:
            return res.json()
        report_error(res)

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
            "flags": 15729697,
            "from": 0,
            "to": 0
        }
        try:
            res = self.session.get(self.BASE_URL + 'core/search_items&params=' + self.convert_params(params) + f"&sid={self.access_token}")
        except (requests.exceptions.ConnectionError, NameResolutionError, TimeoutError) as exception:
            self.session = requests.session()
            raise exception.__class__()
        if 200 <= res.status_code < 300:
            return res.json()
        report_error(res)

    def get_messages(self):
        pass

    def get_historical_messages_by_id(self, item_id, start_ts, end_ts):
        if not self.is_connected():
            self.auth()
        params = {
            'itemId': item_id,
            'timeFrom': start_ts,
            'timeTo': end_ts,
            'flags': 1,
            'flagsMask': 65281,
            'loadCount': 4294967295
        }
        try:
            res = self.session.get(self.BASE_URL + 'messages/load_interval&params=' + json.dumps(params) + f"&sid={self.access_token}")
        except (requests.exceptions.ConnectionError, NameResolutionError, TimeoutError) as exception:
            self.session = requests.session()
            raise exception.__class__()
        if 200 <= res.status_code < 300:
            return res.json()
        report_error(res)
        return res.json()

    def is_connected(self):
        if not self.access_token:
            return False
        return True

    def auth(self):
        params = {"token": self.refresh_token}
        try:
            res = self.session.get(self.BASE_URL + 'token/login&params=' + json.dumps(params))
        except (requests.exceptions.ConnectionError, NameResolutionError, TimeoutError) as exception:
            self.session = requests.session()
            raise exception.__class__()
        if 200 <= res.status_code < 300:
            self.update_token(res.json())
            return True
        report_error(res)
        return False

    def update_token(self, token_params: dict):
        self.access_token = token_params['eid']
        print(self.access_token)

    def reinitialize_session(self, *args):
        self.session = requests.session()
        self.manage_session_units(args)

    def get_counters_info(self):
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
            "flags": 8193,
            "from": 0,
            "to": 0
        }
        try:
            res = self.session.get(self.BASE_URL + 'core/search_items&params=' + self.convert_params(params) + f"&sid={self.access_token}")
        except (requests.exceptions.ConnectionError, NameResolutionError, TimeoutError) as exception:
            self.session = requests.session()
            raise exception.__class__()
        if 200 <= res.status_code < 300:
            return res.json()
        report_error(res)

    def manage_session_units(self, item_ids):
        if not self.is_connected():
            self.auth()
        params = {
            "spec":[
                {
                    "type": "col",
                    "data": item_ids,
                    "flags": 32+8192,
                    "mode": 0
                }
            ]
        }
        try:
            res = self.session.get(self.BASE_URL + 'core/update_data_flags&params=' + self.convert_params(params) + f"&sid={self.access_token}")
        except (requests.exceptions.ConnectionError, NameResolutionError, TimeoutError) as exception:
            self.session = requests.session()
            raise exception.__class__()
        if 200 <= res.status_code < 300:
            return res.json()
        report_error(res)
        return res.json()

    def get_avl_event(self):
        if not self.is_connected():
            self.auth()
        try:
            res = self.session.get(f'https://hst-api.wialon.com/avl_evts?sid={self.access_token}')
        except (requests.exceptions.ConnectionError, NameResolutionError, TimeoutError) as exception:
            self.session = requests.session()
            raise exception.__class__()
        if 200 <= res.status_code < 300:
            return res.json()
        report_error(res)
        return res.json()

    @staticmethod
    def convert_params(params):
        return json.dumps(params)
