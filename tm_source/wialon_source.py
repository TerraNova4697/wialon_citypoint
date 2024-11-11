import requests
import json
import os
import logging

from urllib3.exceptions import NameResolutionError

from tm_source.abs_transport_src import AbstractTransportSource


logger = logging.getLogger(os.environ.get('LOGGER'))


class WialonSource(AbstractTransportSource):

    def __init__(self, login = None, client_id = None, secret_key = None, password = None, access = None):
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
        logger.warning(f"Could not fetch transport states. Status code: {res.status_code}")
        logger.warning(f"Message: {res.json()}")

    def reinitialize_session(self, transport_ids):
        self.session = requests.session()
        self.manage_session_units(transport_ids)

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
        try:
            res = self.session.get(self.BASE_URL + 'core/search_items&params=' + self.convert_params(params) + f"&sid={self.access_token}")
        except (requests.exceptions.ConnectionError, NameResolutionError, TimeoutError) as exception:
            self.session = requests.session()
            raise exception.__class__()
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
            logger.info(res.json())
            return res.json()
        logger.warning(f"Could not fetch transport states. Status code: {res.status_code}")
        logger.warning(f"Message: {res.json()}")

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
            logger.info(res.json())
            return res.json()

    def load_violations_by_id(self, start_ts, item_id):
        if not self.is_connected():
            self.auth()
        params = {
            "itemId": item_id,
            "lastTime": start_ts,
            "lastCount": 500,
            "flags": 1537,
            "flagsMask": 65281,
            'loadCount': 500
        }
        try:
            res = self.session.get(
                self.BASE_URL + "messages/load_last&params=" + self.convert_params(params) + f"&sid={self.access_token}")
        except (requests.exceptions.ConnectionError, NameResolutionError, TimeoutError) as exception:
            self.session = requests.session()
            raise exception.__class__()
        if 200 <= res.status_code < 300:
            return res.json()
        logger.warning(f"Could not fetch violations. Status code: {res.status_code}")
        logger.warning(f"Message: {res.json()}")
        return res.json()

    def get_violations(self):
        if not self.is_connected():
            self.auth()
        params = {
            "indexFrom": 0,
            "indexTo": 9
        }
        try:
            res = self.session.get(
                self.BASE_URL + "messages/get_messages&params=" + self.convert_params(
                    params) + f"&sid={self.access_token}")
        except (requests.exceptions.ConnectionError, NameResolutionError, TimeoutError) as exception:
            self.session = requests.session()
            raise exception.__class__()
        print(res.status_code)
        print(res.json())

    def get_messages(self):
        if not self.is_connected():
            self.auth()
        params = {
            "lang": "ru",
            "measure": "si",
            "detalization": 55
        }
        try:
            res = self.session.get(self.BASE_URL + "events/check_updates&params=" + self.convert_params(params) + f"&sid={self.access_token}")
        except (requests.exceptions.ConnectionError, NameResolutionError, TimeoutError) as exception:
            self.session = requests.session()
            raise exception.__class__()
        if 200 <= res.status_code < 300:
            return res.json()
        logger.warning(f"Could not fetch messages. Status code: {res.status_code}")
        logger.warning(f"Message: {res.json()}")

    def get_messages_from_session(self):
        if not self.is_connected():
            self.auth()
        params = {
            "itemId": 30,
        }
        try:
            res = self.session.get(self.BASE_URL + f'resource/get_notification_data&params={self.convert_params(params)}' + f"&sid={self.access_token}")
        except (requests.exceptions.ConnectionError, NameResolutionError, TimeoutError) as exception:
            self.session = requests.session()
            raise exception.__class__()
        if 200 <= res.status_code < 300:
            return res.json()
        logger.warning(f"Could not fetch transport states. Status code: {res.status_code}")
        logger.warning(f"Message: {res.json()}")

    def read_zones(self):
        if not self.is_connected():
            self.auth()
        res = self.session.get(self.BASE_URL + 'core/search_items&params&params={}' + f"&sid={self.access_token}")

    def unload(self):
        if not self.is_connected():
            self.auth()
        try:
            res = self.session.get(self.BASE_URL + 'messages/unload&params={}' + f"&sid={self.access_token}")
        except (requests.exceptions.ConnectionError, NameResolutionError, TimeoutError) as exception:
            self.session = requests.session()
            raise exception.__class__()
        if 200 <= res.status_code < 300:
            return res.json()
        logger.warning(f"Could not fetch transport states. Status code: {res.status_code}")
        logger.warning(f"Message: {res.json()}")
        return res.json()

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
        logger.warning(f"Could not fetch transport states. Status code: {res.status_code}")
        logger.warning(f"Message: {res.json()}")
        return res.json()

    def load_historical_messages_by_id(self, item_id, start_ts, end_ts):
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
        logger.warning(f"Could not fetch avl events. Status code: {res.status_code}")
        logger.warning(f"Message: {res.json()}")
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
        logger.warning(f"Could not fetch avl events. Status code: {res.status_code}")
        logger.warning(f"Message: {res.json()}")
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
        logger.warning(f"Could not authenticate. Status code: {res.status_code}")
        logger.warning(f"Message: {res.json()}")
        return False

    def update_token(self, token_params: dict):
        self.access_token = token_params['eid']
        print(self.access_token)

    @staticmethod
    def convert_params(params):
        return json.dumps(params)
