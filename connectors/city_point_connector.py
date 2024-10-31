import os
import logging
import asyncio
from abc import ABC

from datetime import datetime, timedelta
from copy import copy

from tb_gateway_mqtt import TBGatewayMqttClient

from connectors.abs_connector import AbstractConnector
from telemetry_objects.transport import Transport
from tm_source.abs_transport_src import AbstractTransportSource
from mqtt_client.abs_destination import AbstractDestination

from database.operations import get_all_sensors, add_sensors_if_not_exist, get_fuel_sensors_ids, get_all_cars_ids, \
    add_transport_if_not_exists, save_unsent_telemetry, get_sensors_by_destination


logger = logging.getLogger(os.environ.get('LOGGER'))


class CityPointConnector(AbstractConnector):


    def __init__(
        self,
        source: AbstractTransportSource,
        destination: AbstractDestination | None,
        data = None
    ):
        super().__init__(source, destination, data)

    async def start_loop(self):
        while not self.source.auth():
            logger.info('Failed authentication')
            await asyncio.sleep(10)
        logger.info('Authenticated')
        asyncio.create_task(self.check_transport_with_discreteness(86400))
        asyncio.create_task(self.fetch_timezones(86400))
        asyncio.create_task(self.fetch_transport_states(16))

    async def check_transport_with_discreteness(self, discreteness: int):

        logger.info("start check_transport_with_discreteness")
        while True:
            transports_result = self.source.get_transport_list()
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
        self.data['transports_id'] = get_all_cars_ids()
        sensors = self.source.get_sensors()
        add_sensors_if_not_exist(sensors['data'])
        self.data['fuel_sensors_id'] = get_sensors_by_destination(100)
        self.data['ignition_sensors_id'] = get_sensors_by_destination(1)
        self.data['light_sensors_id'] = get_sensors_by_destination(1300)

        while True:
            time_format = "%Y-%m-%dT%H:%M:%SZ"
            dt = datetime.now()
            logger.info('Fetching states')
            ts = datetime.timestamp(dt)
            transports = self.source.get_transports(query_filter=','.join(f'"{str(car_id)}"' for car_id in self.data['transports_id']))

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
                        ts=datetime.timestamp(datetime.strptime(transport['attributes']['RecordDate'], time_format) + timedelta(hours=5)),
                        is_sent=False,
                        latitude=transport['attributes']['Lat'],
                        longitude=transport['attributes']['Lon'],
                        velocity=int(velocity_can[0]['value']) if velocity_can else transport['attributes']['Velocity'],
                        fuel_level=fuel_sensor[0]['value'] if fuel_sensor else None,
                        car_id=int(transport['id']),
                        ignition=ignition[0]['value'],
                        light=light[0]['value'],
                        last_conn=datetime.timestamp(datetime.strptime(transport['attributes']['LattestGpsDate'], time_format) + timedelta(hours=5)),
                        name=f"{reg_number} {model}"
                    )

                    if not self.destination.send_data(*t.form_mqtt_message()):
                        pass
                        # TODO: Save telemetry to DB.

                    telemetry.append(t)

            # if not self.destination or not self.destination.send_data(telemetry):
            #     logger.info("DATA SENT CITY POINT")
            #     save_unsent_telemetry(telemetry)

            await asyncio.sleep(discreteness)

    def load_transport_in_memory(self, transports):
        self.transport_map = {str(transport['id']):transport for transport in transports}
