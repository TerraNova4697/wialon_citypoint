import os
import logging
import asyncio
import re
from abc import ABC

from datetime import datetime, timedelta
from copy import copy

from tb_gateway_mqtt import TBGatewayMqttClient
from tb_rest_client import RestClientPE
from urllib3.exceptions import NameResolutionError

from connectors.abs_connector import AbstractConnector
from mqtt_client.cuba_rest_client import CubaRestClient
from telemetry_objects.alarm import Alarm
from telemetry_objects.transport import Transport
from tm_source.abs_transport_src import AbstractTransportSource
from mqtt_client.abs_destination import AbstractDestination

from database.operations import get_all_sensors, add_sensors_if_not_exist, get_fuel_sensors_ids, get_all_cars_ids, \
    add_transport_if_not_exists, save_unsent_telemetry, get_sensors_by_destination


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
        except (ConnectionError, NameResolutionError, TimeoutError) as exc:
            res = False
            logger.exception(f"Exception trying to authenticate: {exc}")
        while not res:
            logger.info('Failed authentication')
            await asyncio.sleep(10)
            await self.start_loop()
        logger.info('Authenticated')

        self.data['transports_id'] = get_all_cars_ids()
        asyncio.create_task(self.fetch_sensors())

        asyncio.create_task(self.check_transport_with_discreteness(86400))
        asyncio.create_task(self.fetch_timezones(86400))
        asyncio.create_task(self.fetch_transport_states(16))

    async def fetch_sensors(self):
        try:
            sensors = self.source.get_sensors()
            add_sensors_if_not_exist(sensors['data'])
            self.data['fuel_sensors_id'] = get_sensors_by_destination(100)
            self.data['ignition_sensors_id'] = get_sensors_by_destination(1)
            self.data['light_sensors_id'] = get_sensors_by_destination(1300)
        except (ConnectionError, NameResolutionError, TimeoutError) as exc:
            logger.exception(f"Exception trying to fetch sensors: {exc}")
            await asyncio.sleep(10)
            await self.fetch_sensors()

    async def fetch_notifications(self, discreteness):
        while True:
            await asyncio.sleep(discreteness)
            try:
                notifications = self.source.get_messages()
            except (ConnectionError, NameResolutionError, TimeoutError) as exc:
                logger.exception(f"Exception trying to fetch notifications: {exc}")
                await asyncio.sleep(10)
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

                if not self.rest_client or not self.rest_client.post_alarm(a):
                    # TODO: Save to DB.
                    pass
                    # save_unsent_alarm_(a)


    async def check_transport_with_discreteness(self, discreteness: int):

        logger.info("start check_transport_with_discreteness")
        while True:
            try:
                transports_result = self.source.get_transport_list()
            except (ConnectionError, NameResolutionError, TimeoutError) as exc:
                logger.exception(f"Exception trying to fetch transport: {exc}")
                await asyncio.sleep(10)
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
            time_format = "%Y-%m-%dT%H:%M:%SZ"
            dt = datetime.now()
            logger.info('Fetching states')
            ts = datetime.timestamp(dt)

            try:
                transports = self.source.get_transports(query_filter=','.join(f'"{str(car_id)}"' for car_id in self.data['transports_id']))
            except (ConnectionError, NameResolutionError, TimeoutError) as exc:
                logger.exception(f"Exception trying to fetch transport stated: {exc}")
                await asyncio.sleep(10)
                continue

            telemetry = []
            logger.info(f"Fetched {len(transports['data'])} units")
            for transport in transports['data']:

                if int(transport['id']) in self.data['transports_id']:
                    fuel_sensor = [
                        sensor for sensor in transport['attributes']['Sensors'] if sensor['id'] in self.data['fuel_sensors_id']
                    ]
                    ignition = [sensor for sensor in transport['attributes']['Sensors'] if sensor['id'] == 1]
                    light = [sensor for sensor in transport['attributes']['Sensors'] if sensor['id'] == 104]
                    velocity_can = [sensor for sensor in transport['attributes']['Sensors'] if sensor['id'] == 41]
                    transport_model = self.transport_map.get(str(transport['id']))
                    model = transport_model.get('attributes', {}).get('Model', '')
                    reg_number = transport_model.get('attributes', {}).get('RegNumber', '').replace('_', ' ')
                    t = Transport(
                        ts=datetime.timestamp(datetime.strptime(transport['attributes']['RecordDate'], time_format)),
                        is_sent=False,
                        latitude=transport['attributes']['Lat'],
                        longitude=transport['attributes']['Lon'],
                        velocity=int(velocity_can[0]['value']) if velocity_can else transport['attributes']['Velocity'],
                        fuel_level=fuel_sensor[0]['value'] if fuel_sensor else None,
                        car_id=int(transport['id']),
                        ignition=ignition[0]['value'],
                        light=light[0]['value'],
                        last_conn=datetime.timestamp(datetime.strptime(transport['attributes']['LattestGpsDate'], time_format)),
                        name=f"{reg_number} {model}"
                    )

                    if not self.destination or not self.destination.send_data(*t.form_mqtt_message()):
                        save_unsent_telemetry(t)


            await asyncio.sleep(discreteness)

    def load_transport_in_memory(self, transports):
        self.transport_map = {str(transport['id']):transport for transport in transports}
