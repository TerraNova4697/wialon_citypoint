import os
import logging
import asyncio
import re
from abc import ABC

from datetime import datetime, timedelta
from copy import copy
from http.client import RemoteDisconnected

from tb_gateway_mqtt import TBGatewayMqttClient
from tb_rest_client import RestClientPE
from urllib3.exceptions import NameResolutionError, MaxRetryError
from requests.exceptions import ConnectionError as RequestsConnectionError

from connectors.abs_connector import AbstractConnector
from mqtt_client.cuba_rest_client import CubaRestClient
from telemetry_objects.alarm import Alarm
from telemetry_objects.transport import Transport
from tm_source.abs_transport_src import AbstractTransportSource
from mqtt_client.abs_destination import AbstractDestination

from database.operations import get_all_sensors, add_sensors_if_not_exist, get_fuel_sensors_ids, get_all_cars_ids, \
    add_transport_if_not_exists, save_unsent_telemetry, get_sensors_by_destination, get_car_by_id, get_last_runtime, \
    get_transport_ids

logger = logging.getLogger(os.environ.get('LOGGER'))


def full_date_to_timestamp(date: str) -> int:
    time_format = "%Y-%m-%dT%H:%M:%SZ"
    return int(datetime.timestamp(datetime.strptime(date, time_format)))


def remove_html_tags(text):
    # Use regex to remove HTML tags
    clean_text = re.sub(r'<.*?>', '', text)
    return clean_text


class CityPointConnector(AbstractConnector):


    def __init__(
        self,
        source: AbstractTransportSource,
        destination: AbstractDestination | None,
        data = None,
        rest_client: CubaRestClient = None
    ):
        super().__init__(source, destination, data)
        self.rest_client: CubaRestClient | None = rest_client

    async def start_loop(self):
        try:
            res = self.source.auth()
        except (RequestsConnectionError, NameResolutionError, TimeoutError, RemoteDisconnected) as exc:
            res = False
            logger.exception(f"Exception trying to authenticate: {exc}")
        while not res:
            logger.info('Failed authentication')
            await asyncio.sleep(30)
            await self.start_loop()
        logger.info('Authenticated')

        self.data['transports_id'] = get_all_cars_ids()
        asyncio.create_task(self.fetch_sensors())

        asyncio.create_task(self.check_transport_with_discreteness(86400))
        asyncio.create_task(self.fetch_timezones(86400))
        # if runtime := get_last_runtime():
        #     asyncio.create_task(self.get_states_since(runtime))
        asyncio.create_task(self.fetch_transport_states(16))
        asyncio.create_task(self.daily_report(hour=6, minute=0))

    async def daily_report(self, hour, minute):
        while True:
            # Get the current time
            now = datetime.now()
            next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

            if next_run < now:
                next_run += timedelta(days=1)

            time_to_wait = (next_run - now).total_seconds()
            logger.info(f"Waiting {time_to_wait} seconds until the next run at {next_run}")

            await asyncio.sleep(time_to_wait)

            await self.send_report()

    async def send_report(self):
        dt = datetime.today().replace(hour=6, minute=0, second=0, microsecond=0) - timedelta(days=1)
        try:
            res = self.source.get_day_info(dt.strftime('%Y-%m-%d'))
        except (RequestsConnectionError, NameResolutionError, TimeoutError, RemoteDisconnected) as exc:
            logger.exception(f"Exception trying to fetch day report: {exc}")
            await asyncio.sleep(10)
        except MaxRetryError:
            self.source.delay = 60
            await self.send_report()

        if not res or not res.get('data'):
            return
        for record in res['data']:
            car = get_car_by_id(int(record['relationships']['Car']['data']['id']))
            if not car or car.is_hidden:
                continue

            try:
                self.destination.send_data(
                    car.name,
                    {
                        'ts': int(round(datetime.timestamp(dt) * 1000)),
                        'values': {
                            'mileage': record['attributes'].get('Mileage', 0),
                            'working_hours': record['attributes'].get('WorkingHours', 0)
                        }
                    }
                )
            except Exception as exc:
                logger.exception(exc)

    async def get_states_since(self, runtime):
        start_ts, end_ts = runtime.end_ts, int(datetime.timestamp(datetime.now()))

        try:
            transport_ids = get_transport_ids('city_point')
            print(transport_ids)
            for transport_id in transport_ids:
                res = self.source.load_historical_messages_by_id(transport_id, start_ts, end_ts)
                self.save_trips(transport_id, res.get('messages', []))
                await asyncio.sleep(10)

        except Exception as exc:
            logger.exception(exc)

    def save_trips(self, transport_id, trips):
        car = get_car_by_id(transport_id)
        time_format = "%Y-%m-%dT%H:%M:%SZ"
        print(f"for {car.name} {len(trips)} states")
        for trip in trips:
            save_unsent_telemetry(Transport(
                ts=datetime.strptime(trip['attributes']['RecordDate'], time_format),
                is_sent=False,
                latitude=trip['attributes']['Lat'],
                longitude=trip['attributes']['Lat'],
                velocity=trip['attributes']['Velocity'],
                fuel_level=None,
                car_id=transport_id,
                ignition=1,
                light=None,
                last_conn=datetime.strptime(trip['attributes']['RecordDate'], time_format),
                name=car.name
            ))

    async def fetch_sensors(self):
        try:
            sensors = self.source.get_sensors()
            add_sensors_if_not_exist(sensors['data'])
            self.data['fuel_sensors_id'] = get_sensors_by_destination(100)
            self.data['ignition_sensors_id'] = get_sensors_by_destination(1)
            self.data['light_sensors_id'] = get_sensors_by_destination(1300)
        except (RequestsConnectionError, NameResolutionError, TimeoutError, RemoteDisconnected) as exc:
            logger.exception(f"Exception trying to fetch sensors: {exc}")
            await asyncio.sleep(10)
        except MaxRetryError:
            self.source.delay = 60
            await self.fetch_sensors()

    async def fetch_notifications(self, discreteness):
        while True:
            await asyncio.sleep(discreteness)
            try:
                notifications = self.source.get_messages()
            except (RequestsConnectionError, NameResolutionError, TimeoutError, RemoteDisconnected) as exc:
                logger.exception(f"Exception trying to fetch notifications: {exc}")
                await asyncio.sleep(10)
                continue
            except MaxRetryError:
                self.source.delay = 60
                continue
            alarms = [alarm for alarm in notifications['data'] if alarm['attributes']['Level'] >= 4]
            cars = [car for car in notifications['included'] if car['type'] == 'car']
            drivers = [driver for driver in notifications['included'] if driver['type'] == 'driver']
            if not alarms:
                continue
            # TODO: save alarms to DB.
            alarm_objects = []
            for alarm in alarms:
                message = remove_html_tags(alarm['attributes']['Message']).split('\\')[0]
                car_id = alarm['relationships'].get('Car', {}).get('data', {}).get('id')
                driver_id = alarm['relationships'].get('Driver', {}).get('data', {}).get('id')
                driver = [driver for driver in drivers if driver['id'] == driver_id]
                a = Alarm(
                    id=int(alarm['id']),
                    title=alarm['attributes']['Title'],
                    message=message,
                    level=alarm['attributes']['Level'],
                    latitude=alarm['attributes']['Latitude'],
                    longitude=alarm['attributes']['Longitude'],
                    record_date=full_date_to_timestamp(alarm['attributes']['RecordDate']),
                    date_of_creation=full_date_to_timestamp(alarm['attributes']['DateOfCreation']),
                    car_id=car_id,
                    driver_first_name=driver['attributes']['FIO']['FirstName'] if driver else '',
                    driver_last_name=driver['attributes']['FIO']['LastName'] if driver else '',
                    place=None
                )

                car = get_car_by_id(a.car_id)

                if not self.rest_client or not self.rest_client.post_alarm(a, car.name):
                    # TODO: Save to DB.
                    pass
                    # save_unsent_alarm_(a)

    async def check_transport_with_discreteness(self, discreteness: int):

        logger.info("start check_transport_with_discreteness")
        while True:
            try:
                transports_result = self.source.get_transport_list()
            except (RequestsConnectionError, NameResolutionError, TimeoutError, RemoteDisconnected) as exc:
                logger.exception(f"Exception trying to fetch transport: {exc}")
                await asyncio.sleep(10)
                continue
            except MaxRetryError:
                self.source.delay = 60
                continue
            transports = transports_result['data']
            add_transport_if_not_exists(transports)
            self.load_transport_in_memory(transports)
            await asyncio.sleep(discreteness)
            logger.info("end check_transport_with_discreteness")

    async def fetch_timezones(self, discreteness: int):
        logger.info("start fetch_timezones")
        while True:
            await asyncio.sleep(discreteness)
            logger.info('end fetch_timezones')

    async def fetch_transport_states(self, discreteness: int):
        while True:
            logger.info('Fetching states')
            time_format = "%Y-%m-%dT%H:%M:%SZ"
            dt = datetime.now()
            ts = datetime.timestamp(dt)

            try:
                transports = self.source.get_transports(query_filter=','.join(f'"{str(car_id)}"' for car_id in self.data['transports_id']))
                if transports and transports.get('errors'):
                    logger.warning(transports)
                    await asyncio.sleep(10)
                    continue
            except (RequestsConnectionError, NameResolutionError, TimeoutError, RemoteDisconnected) as exc:
                logger.exception(f"Exception trying to fetch transport stated: {exc}")
                await asyncio.sleep(10)
                continue
            except MaxRetryError:
                self.source.delay = 60
                continue

            for transport in transports.get('data', []):
                if int(transport['id']) in self.data['transports_id']:
                    latest_gps_date = datetime.strptime(transport['attributes']['LattestGpsDate'], time_format)
                    if (datetime.now() - latest_gps_date).seconds > 600:
                        continue
                    fuel_sensor = [
                        sensor for sensor in transport['attributes']['Sensors'] if sensor['id'] in self.data['fuel_sensors_id']
                    ]
                    ignition = [sensor for sensor in transport['attributes']['Sensors'] if sensor['id'] == 1]
                    light = [sensor for sensor in transport['attributes']['Sensors'] if sensor['id'] == 104]
                    velocity_can = [sensor for sensor in transport['attributes']['Sensors'] if sensor['id'] == 41]
                    transport_model = self.transport_map.get(str(transport['id']))
                    model = transport_model.get('attributes', {}).get('Model', '')
                    reg_number = transport_model.get('attributes', {}).get('RegNumber', '').replace('_', ' ')
                    ts = datetime.strptime(transport['attributes']['RecordDate'], time_format)
                    last_conn = datetime.strptime(transport['attributes']['LattestGpsDate'], time_format)
                    t = Transport(
                        ts=datetime.timestamp(ts),
                        is_sent=False,
                        latitude=transport['attributes']['Lat'],
                        longitude=transport['attributes']['Lon'],
                        velocity=int(velocity_can[0]['value']) if velocity_can else transport['attributes']['Velocity'],
                        fuel_level=fuel_sensor[0]['value'] if fuel_sensor else None,
                        car_id=int(transport['id']),
                        ignition=ignition[0]['value'],
                        light=light[0]['value'],
                        last_conn=datetime.timestamp(last_conn),
                        name=get_car_by_id(int(transport['id'])).name
                    )

                    if not self.destination or not self.destination.send_data(*t.form_mqtt_message()):
                        save_unsent_telemetry(t)

            await asyncio.sleep(discreteness)

    def load_transport_in_memory(self, transports):
        self.transport_map = {str(transport['id']):transport for transport in transports}
