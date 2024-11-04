import os
import logging
from time import sleep

from tm_source.abs_transport_src import AbstractTransportSource
from datetime import datetime, timedelta
import requests
from functools import wraps

import jwt


logger = logging.getLogger(os.environ.get('LOGGER'))


class CityPointSource(AbstractTransportSource):

    def __init__(self, login, client_id, secret_key, password, *args, **kwargs):
        super().__init__(login, client_id, secret_key, password)
        self.refresh_token: str | None = None
        self.access_token: str | None = None
        self.expires_at: datetime | None = None
        self.token_type: str | None = None
        self.user_id: str | None = None
        self.BASE_URL: str = "https://api.citypoint.ru/v2.1"
        self.AUTH_URL: str = "/oauth/token"
        self.TS_INFO: str = f"/cars/states?fields[carState]=Lon,Lat,Velocity,RecordDate,LattestGpsDate,Sensors.value,Sensors.calibration"
        self.TS_LIST: str = f"/cars?filter[car]=eq(IsHidden,0)"
        self.SENSORS_INFO: str = f"/sensors"
        self.MESSAGES: str = '/notifications?include=Driver,Zone,Car&page[limit]=10'
        self.GEO_ZONES: str = '/zones?fields[zone]=Name,Description,Geometry'

    def get_planned_routes(self):
        pass

    def get_incidents(self):
        pass

    def get_velocity_zones(self):
        self.get_token_if_expired()
        headers = {
            "Accept": "application/vnd.api+json",
            "Authorization": f"{self.token_type} {self.access_token}"
        }
        res = requests.get(
            url=self.BASE_URL + f"/user/{self.user_id}" + self.SENSORS_INFO,
            headers=headers
        )
        if 200 <= res.status_code < 300:
            return res.json()
        logger.warning(f"Could not fetch geo-zones. Status code: {res.status_code}")
        logger.warning(f"Message: {res.json()}")

    def get_token_if_expired(self):
        if not self.is_connected():
            while not self.get_access_token():
                sleep(10)

    def get_sensors(self):
        self.get_token_if_expired()
        headers = {
            "Accept": "application/vnd.api+json",
            "Authorization": f"{self.token_type} {self.access_token}"
        }
        res = requests.get(
            url=self.BASE_URL + self.SENSORS_INFO,
            headers=headers
        )
        if 200 <= res.status_code < 300:
            return res.json()
        logger.warning(f"Could not fetch sensors. Status code: {res.status_code}")
        logger.warning(f"Message: {res.json()}")

    def get_transport_list(self):
        self.get_token_if_expired()
        headers = {
            "Accept": "application/vnd.api+json",
            "Authorization": f"{self.token_type} {self.access_token}"
        }
        res = requests.get(
            url=self.BASE_URL + f"/user/{self.user_id}" + self.TS_LIST,
            headers=headers
        )
        if 200 <= res.status_code < 300:
            return res.json()
        logger.warning(f"Could not fetch transport list. Status code: {res.status_code}")
        logger.warning(f"Message: {res.json()}")

    def get_transports(self, query_filter: str = ''):
        self.get_token_if_expired()
        headers = {
            "Accept": "application/vnd.api+json",
            "Authorization": f"{self.token_type} {self.access_token}"
        }

        res = requests.get(
            url=self.BASE_URL + f"/user/{self.user_id}" + self.TS_INFO,
            headers=headers
        )
        if 200 <= res.status_code < 300:
            return res.json()
        logger.warning(f"Could not fetch transport states. Status code: {res.status_code}")
        logger.warning(f"Message: {res.json()}")

    def update_token(self, token_params: dict):
        self.token_type = token_params['token_type']
        self.expires_at = datetime.now() + timedelta(seconds=token_params['expires_in'])
        self.access_token = token_params['access_token']
        self.refresh_token = token_params['refresh_token']
        token = jwt.decode(self.access_token, options={"verify_signature": False})
        self.user_id = token.get('user_id')

    def auth(self):
        res = requests.post(
            url=self.BASE_URL + self.AUTH_URL,
            data=f"grant_type=password&client_id={self.client_id}&client_secret={self.secret_key}&username={self.login}&password={self.password}",
            headers={
                "Content-Type": "application/x-www-form-urlencoded"
            }
        )
        if 200 <= res.status_code < 300:
            self.update_token(res.json())
            return True
        logger.warning(f"Could not authenticate. Status code: {res.status_code}")
        logger.warning(f"Message: {res.json()}")
        return False


    def get_access_token(self):
        res = requests.post(
            url=self.BASE_URL + self.AUTH_URL,
            data=f"grant_type=refresh_token&client_id={self.client_id}&client_secret={self.secret_key}&refresh_token={self.refresh_token}",
            headers={
                "Content-Type": "application/x-www-form-urlencoded"
            }
        )
        if 200 <= res.status_code < 300:
            self.update_token(res.json())
            return True
        logger.warning(f"Could not update token. Status code: {res.status_code}")
        logger.warning(f"Message: {res.json()}")

    def get_messages(self):
        self.get_token_if_expired()
        headers = {
            "Accept": "application/vnd.api+json",
            "Authorization": f"{self.token_type} {self.access_token}"
        }
        res = requests.get(
            url=self.BASE_URL + f"/user/{self.user_id}" + self.MESSAGES,
            headers=headers
        )
        if 200 <= res.status_code < 300:
            return res.json()
        logger.warning(f"Could not fetch messages. Status code: {res.status_code}")
        logger.warning(f"Message: {res.json()}")

    def is_connected(self):
        return datetime.now() > self.expires_at