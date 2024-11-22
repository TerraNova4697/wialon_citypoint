"""Starting point for working with City Point monitoring system"""
from time import sleep

from urllib3.exceptions import NameResolutionError
from http.client import RemoteDisconnected

from monitoring_source.abs_transport_src import AbstractTransportSource
from datetime import datetime, timedelta
import requests

import jwt

from monitoring_source.utils import report_error


class CityPointSource(AbstractTransportSource):

    def __init__(self, login, client_id, secret_key, password):
        super().__init__(login, client_id, secret_key, password)
        self.refresh_token: str | None = None
        self.access_token: str | None = None
        self.expires_at: datetime | None = None
        self.token_type: str | None = None
        self.user_id: str | None = None
        self.session = requests.session()
        self.delay: int | None = None
        self.BASE_URL: str = "https://api.citypoint.ru/v2.1"

        # Endpoints for REST API
        self.AUTH_URL: str = "/oauth/token"
        self.TS_INFO: str = f"/cars/states?fields[carState]=Lon,Lat,Velocity,RecordDate,LattestGpsDate,LattestConnectionTime,Sensors.value,Sensors.calibration"
        self.TS_LIST: str = f"/cars?filter[car]=eq(IsHidden,0)"
        self.SENSORS_INFO: str = f"/sensors"
        self.MESSAGES: str = '/notifications?include=Driver,Zone,Car&page[limit]=10'
        self.GEO_ZONES: str = '/zones?fields[zone]=Name,Description,Geometry'
        self.DAY_CAR_INFO: str = '/cars/aggregated/{}/day?fields[carAggrData]=Mileage,WorkingHours,FuelConsumptionHour,FuelConsumptionKm,IdleFuelVolume,IdleHours,Car'

    def get_velocity_zones(self) -> dict | None:
        """
        Get all geo zones
        :return:
        """
        self.get_token_if_expired()
        headers = self.header
        try:
            res = self.session.get(
                url=self.BASE_URL + f"/user/{self.user_id}" + self.SENSORS_INFO,
                headers=headers
            )
        except (requests.exceptions.ConnectionError, NameResolutionError, TimeoutError, RemoteDisconnected) as exception:
            self.session = requests.session()
            raise exception.__class__()
        if 200 <= res.status_code < 300:
            return res.json()
        report_error(res)

    def get_transport_list(self) -> dict | None:
        """
        Get list of transport. Return dict if request successful. None if status code != 2**
        :return:
        """
        self.get_token_if_expired()
        headers = self.header
        try:
            res = self.session.get(
                url=self.BASE_URL + f"/user/{self.user_id}" + self.TS_LIST,
                headers=headers
            )
        except (requests.exceptions.ConnectionError, NameResolutionError, TimeoutError, RemoteDisconnected) as exception:
            self.session = requests.session()
            raise exception.__class__()
        if 200 <= res.status_code < 300:
            return res.json()
        report_error(res)

    def get_transports(self, query_filter: str = '') -> dict | None:
        """
        Get list of current transport states. Return dict if request successful. None if status code != 2**
        :param query_filter:
        :return:
        """
        self.get_token_if_expired()
        headers = self.header
        try:
            res = self.session.get(
                url=self.BASE_URL + f"/user/{self.user_id}" + self.TS_INFO,
                headers=headers
            )
        except (requests.exceptions.ConnectionError, NameResolutionError, TimeoutError, RemoteDisconnected) as exception:
            self.session = requests.session()
            raise exception.__class__()
        if 200 <= res.status_code < 300:
            return res.json()
        report_error(res)

    def get_historical_messages_by_id(self, transport_id, start_ts, end_ts) -> dict | None:
        """
        Get list of historical transport states for given transport ID. Return dict if request successful. None if status code != 2**
        :param transport_id:
        :param start_ts:
        :param end_ts:
        :return:
        """
        self.get_token_if_expired()
        dt = datetime.fromtimestamp(start_ts)
        formatted_dt = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        headers = self.header
        try:
            res = self.session.get(
                url=self.BASE_URL + f"/user/{self.user_id}" + f"/cars/{transport_id}/history/full" + f"?fields[histState]=Velocity,Lat,Lon,RecordDate&filter[histState]=and(gte(Velocity,3),gt(RecordDate,{formatted_dt}))",
                headers=headers
            )
        except (requests.exceptions.ConnectionError, NameResolutionError, TimeoutError, RemoteDisconnected) as exception:
            self.session = requests.session()
            raise exception.__class__()
        if 200 <= res.status_code < 300:
            return res.json()
        report_error(res)

    def update_token(self, token_params: dict):
        self.token_type = token_params['token_type']
        self.expires_at = datetime.now() + timedelta(seconds=token_params['expires_in'])
        self.access_token = token_params['access_token']
        self.refresh_token = token_params['refresh_token']
        token = jwt.decode(self.access_token, options={"verify_signature": False})
        self.user_id = token.get('user_id')

    def auth(self) -> bool:
        """
        Authenticate in the system and get access and refresh tokens. Returns True if successful. False otherwise
        :return:
        """
        try:
            res = requests.post(
                url=self.BASE_URL + self.AUTH_URL,
                data=f"grant_type=password&client_id={self.client_id}&client_secret={self.secret_key}&username={self.login}&password={self.password}",
                headers={
                    "Content-Type": "application/x-www-form-urlencoded"
                }
            )
        except (requests.exceptions.ConnectionError, NameResolutionError, TimeoutError, RemoteDisconnected) as exception:
            self.session = requests.session()
            raise exception.__class__()
        if 200 <= res.status_code < 300:
            self.update_token(res.json())
            return True
        report_error(res)
        return False

    def get_messages(self) -> dict | None:
        """
        Get list messages. Return dict if request successful. None if status code != 2**
        :return:
        """
        self.get_token_if_expired()
        headers = self.header
        try:
            res = self.session.get(
                url=self.BASE_URL + f"/user/{self.user_id}" + self.MESSAGES,
                headers=headers
            )
        except (requests.exceptions.ConnectionError, NameResolutionError, TimeoutError, RemoteDisconnected) as exception:
            self.session = requests.session()
            raise exception.__class__()
        if 200 <= res.status_code < 300:
            return res.json()
        report_error(res)

    def is_connected(self) -> bool:
        """
        Check if access token is expired
        :return:
        """
        return datetime.now() > self.expires_at

    def reinitialize_session(self):
        self.session = requests.session()

    @property
    def header(self):
        return {
            "Accept": "application/vnd.api+json",
            "Authorization": f"{self.token_type} {self.access_token}"
        }

    def __get_access_token(self) -> bool:
        """
        Update access token
        :return:
        """
        try:
            res = requests.post(
                url=self.BASE_URL + self.AUTH_URL,
                data=f"grant_type=refresh_token&client_id={self.client_id}&client_secret={self.secret_key}&refresh_token={self.refresh_token}",
                headers={
                    "Content-Type": "application/x-www-form-urlencoded"
                }
            )
        except (requests.exceptions.ConnectionError, NameResolutionError, TimeoutError, RemoteDisconnected) as exception:
            self.session = requests.session()
            raise exception.__class__()
        if 200 <= res.status_code < 300:
            self.update_token(res.json())
            return True
        report_error(res)
        return False

    def get_token_if_expired(self):
        """
        If access token is expired, update it
        :return:
        """
        if not self.is_connected():
            # if self.delay:
            #     sleep(self.delay)
            #     self.delay = None
            while not self.__get_access_token():
                sleep(10)

    def get_day_info(self, date_str: str) -> dict | None:
        """
        Get list of data for the given day. Return dict if request successful. None if status code != 2**
        :param date_str: date string representation with format: YYYY-MM-DD
        :return:
        """
        self.get_token_if_expired()
        headers = self.header
        try:
            res = self.session.get(
                url=self.BASE_URL + f"/user/{self.user_id}" + self.DAY_CAR_INFO.format(date_str),
                headers=headers
            )
        except (requests.exceptions.ConnectionError, NameResolutionError, TimeoutError, RemoteDisconnected) as exception:
            self.session = requests.session()
            raise exception.__class__()
        if 200 <= res.status_code < 300:
            return res.json()
        report_error(res)

    def get_sensors(self) -> dict | None:
        """
        Get list of all sensors. Return dict if request successful. None if status code != 2**
        :return:
        """
        self.get_token_if_expired()
        headers = self.header
        try:
            res = self.session.get(
                url=self.BASE_URL + self.SENSORS_INFO,
                headers=headers
            )
        except (requests.exceptions.ConnectionError, NameResolutionError, TimeoutError, RemoteDisconnected) as exception:
            self.session = requests.session()
            raise exception.__class__()
        if 200 <= res.status_code < 300:
            return res.json()
        report_error(res)
