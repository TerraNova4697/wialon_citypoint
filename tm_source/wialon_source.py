import requests
import json
import os
import logging

from tm_source.abs_transport_src import AbstractTransportSource


logger = logging.getLogger(os.environ.get('LOGGER'))


class WialonSource(AbstractTransportSource):

    def __init__(self, login = None, client_id = None, secret_key = None, password = None, access = None):
        super().__init__(login, client_id, secret_key, password)
        self.refresh_token: str | None = secret_key
        self.access_token: str | None = None
        self.token_type: str | None = None
        self.user_id: str | None = None
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
        res = requests.get(
            self.BASE_URL + 'core/search_items&params=' + self.convert_params(params) + f"&sid={self.access_token}")
        if 200 <= res.status_code < 300:
            return res.json()
        logger.warning(f"Could not fetch transport states. Status code: {res.status_code}")
        logger.warning(f"Message: {res.json()}")

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
        if 200 <= res.status_code < 300:
            return res.json()
        logger.warning(f"Could not fetch transport list. Status code: {res.status_code}")
        logger.warning(f"Message: {res.json()}")

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
            "flags": 4611686018427387903,
            "from": 0,
            "to": 0
        }
        res = requests.get(self.BASE_URL + 'core/search_items&params=' + self.convert_params(params) + f"&sid={self.access_token}")
        if 200 <= res.status_code < 300:
            return res.json()
        logger.warning(f"Could not fetch transport states. Status code: {res.status_code}")
        logger.warning(f"Message: {res.json()}")

    def get_messages(self):
        if not self.is_connected():
            self.auth()
        params = {
            "spec": {
                "itemsType": "avl_resource",
                "propName": "avl_resource,notifications,drivers",
                "propValueMask": "*,*,*",
                "sortType": "sys_name",
                "propType": "avl_resource,propitemname,propitemname"
            },
            "force": 1,
            "flags": 1281,
            "from": 0,
            "to": 0
        }
        res = requests.get(self.BASE_URL + "core/search_items&params=" + self.convert_params(params) + f"&sid={self.access_token}")
        if 200 <= res.status_code < 300:
            return res.json()
        logger.warning(f"Could not fetch transport states. Status code: {res.status_code}")
        logger.warning(f"Message: {res.json()}")

    def get_messages_from_session(self):
        if not self.is_connected():
            self.auth()
        params = {
            "itemId": 30,
        }
        res = requests.get(self.BASE_URL + f'resource/get_notification_data&params={self.convert_params(params)}' + f"&sid={self.access_token}")
        if 200 <= res.status_code < 300:
            return res.json()
        logger.warning(f"Could not fetch transport states. Status code: {res.status_code}")
        logger.warning(f"Message: {res.json()}")

    def read_zones(self):
        if not self.is_connected():
            self.auth()
        res = requests.get(self.BASE_URL + 'core/search_items&params&params={}' + f"&sid={self.access_token}")

    def unload(self):
        if not self.is_connected():
            self.auth()
        res = requests.get(self.BASE_URL + 'messages/unload&params={}' + f"&sid={self.access_token}")
        if 200 <= res.status_code < 300:
            return res.json()
        return res.json()
        logger.warning(f"Could not fetch transport states. Status code: {res.status_code}")
        logger.warning(f"Message: {res.json()}")

    def load_messages(self):
        if not self.is_connected():
            self.auth()
        params = {
            "itemId":30,
            "timeFrom":1727722800,
            "timeTo":1730314800,
            "flags":1,
            "flagsMask":65281,
            "loadCount":3
        }
        res = requests.get(self.BASE_URL + 'messages/load_interval&params=' + self.convert_params(params) + f"&sid={self.access_token}")
        print(res.status_code)
        print(res.json())
        if 200 <= res.status_code < 300:
            return res.json()
        return res.json()
        logger.warning(f"Could not fetch transport states. Status code: {res.status_code}")
        logger.warning(f"Message: {res.json()}")

    def is_connected(self):
        if not self.access_token:
            return False
        return True


    def auth(self):
        params = {"token": self.refresh_token}
        res = requests.get(self.BASE_URL + 'token/login&params=' + json.dumps(params))
        if 200 <= res.status_code < 300:
            self.update_token(res.json())
            return True
        logger.warning(f"Could not authenticate. Status code: {res.status_code}")
        logger.warning(f"Message: {res.json()}")
        return False

    def update_token(self, token_params: dict):
        self.access_token = token_params['eid']
        print(self.access_token)

    @staticmethod
    def convert_params(params):
        return json.dumps(params)
