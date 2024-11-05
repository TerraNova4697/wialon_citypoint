import asyncio
import json
import os
import logging

from urllib3.exceptions import NameResolutionError

from connectors.abs_connector import AbstractConnector
from mqtt_client.abs_destination import AbstractDestination
from mqtt_client.cuba_rest_client import CubaRestClient
from telemetry_objects.alarm import Alarm
from telemetry_objects.transport import Transport

from database.operations import save_unsent_telemetry, add_wialon_transport_if_not_exists, get_transport_ids, \
    get_car_by_id
from tm_source.abs_transport_src import AbstractTransportSource

logger = logging.getLogger(os.environ.get("LOGGER"))


class WialonConnector(AbstractConnector):

    def __init__(
            self,
            source: AbstractTransportSource,
            destination: AbstractDestination | None,
            data=None,
            rest_client: CubaRestClient = None
    ):
        super().__init__(source, destination, data)
        self.rest_client: CubaRestClient | None = rest_client

    async def start_loop(self):
        try:
            res = self.source.auth()
        except (ConnectionError, NameResolutionError, TimeoutError) as exc:
            logger.exception(f"Exception trying to authenticate: {exc}")
            res = False
        while not res:
            logger.info('Failed authentication')
            await asyncio.sleep(10)
            await self.start_loop()

        wialon_transport_ids = get_transport_ids('wialon')
        # self.source.manage_session_units('remove', wialon_transport_ids)
        # asyncio.create_task(self.fetch_notifications(60))
        asyncio.create_task(self.check_transport_with_discreteness(86400))
        self.source.manage_session_units(wialon_transport_ids)
        # asyncio.create_task(self.fetch_transport_states(16))
        asyncio.create_task(self.get_avls(2))

    async def get_avls(self, discreteness: int):
        while True:
            logger.info('Fetching states')
            try:
                data = self.source.get_avl_event()
            except (ConnectionError, NameResolutionError, TimeoutError) as exc:
                logger.exception(f"Exception trying to fetch transport states: {exc}")
                await asyncio.sleep(10)
                continue
            print(data)
            logger.info(f"Fetched {len(data.get('events', []))} events")


            for event in data.get('events', []):
                try:
                    if event['d']['tp'] == 'ud':
                        await self.parse_transport_state(event)
                    elif event['d']['tp'] == 'evt':
                        await self.parse_violation(event)
                except KeyError as e:
                    logger.exception(event)
                    logger.exception(e)

            await asyncio.sleep(discreteness)

    async def parse_violation(self, event):
        if event['d']['f'] == 1537:
            a = Alarm(
                id=event['i'],
                title=event['d']['et'],
                message=event['d']['p']['ACTIVE_UNACK'],
                level=7,
                latitude=event['d']['y'],
                longitude=event['d']['x'],
                record_date=event['d']['t'],
                date_of_creation=event['d']['p']['task_update_time'],
                car_id=event['i'],
                place=json.loads(event['d']['p'].get('task_tags', '')).get('ZONE', '')
            )
            car = get_car_by_id(a.car_id)
            if not self.rest_client or not self.rest_client.post_alarm(a, car.name):
                # TODO: Save to DB.
                pass

    async def parse_transport_state(self, event):
        if event.get('d', {}).get('pos'):
            t = Transport(
                ts=event['d']['t'],
                is_sent=False,
                latitude=event['d']['pos']['y'],
                longitude=event['d']['pos']['x'],
                velocity=event['d']['pos']['s'],
                fuel_level=None,
                car_id=event['i'],
                ignition=event['d']['pos'].get('io_239'),
                light=None,
                last_conn=event['d']['rt'],
                name=get_car_by_id(event['i']).name
            )
            if not self.destination or not self.destination.send_data(*t.form_mqtt_message()):
                save_unsent_telemetry(t)

    async def fetch_transport_states(self, discreteness: int):
        while True:
            logger.info('Fetching states')
            try:
                data = self.source.get_transports()
            except (ConnectionError, NameResolutionError, TimeoutError) as exc:
                logger.exception(f"Exception trying to fetch transport states: {exc}")
                await asyncio.sleep(10)
                continue
            logger.info(f"Fetched {len(data['items'])} units")
            telemetry = []

            for transport in data['items']:
                if transport['lmsg']['pos']:
                    t = Transport(
                        ts=transport['lmsg']['t'],
                        is_sent=False,
                        latitude=transport['lmsg']['pos']['y'],
                        longitude=transport['lmsg']['pos']['x'],
                        velocity=transport['lmsg']['pos']['s'],
                        fuel_level=None,
                        car_id=transport['id'],
                        ignition=transport['lmsg']['p'].get('io_239'),
                        light=None,
                        last_conn=transport['lmsg']['rt'],
                        name=self.transport_map.get(str(transport['id']))['nm']
                    )
                    telemetry.append(t)
                    if not self.destination or not self.destination.send_data(*t.form_mqtt_message()):
                        save_unsent_telemetry(t)

            await asyncio.sleep(discreteness)

    async def fetch_notifications(self, discreteness):
        while True:
            try:
                events = self.source.get_messages()
                logger.info(json.dumps(events))
            except (ConnectionError, NameResolutionError, TimeoutError) as exc:
                logger.exception(f"Exception trying to fetch updates: {exc}")
                await asyncio.sleep(10)
                continue

            await asyncio.sleep(discreteness)

    async def check_transport_with_discreteness(self, discreteness: int):
        while True:
            try:
                transports_result = self.source.get_transport_list()
            except (ConnectionError, NameResolutionError, TimeoutError) as exc:
                logger.exception(f"Exception trying to fetch transport list: {exc}")
                await asyncio.sleep(10)
                continue
            transports = transports_result['items']
            logger.info(f"Fetched {len(transports)} units")
            transport_props = []
            for transport in transports:

                department = [field['v'] for field in transport['pflds'].values() if field['n'] == 'vehicle_type']
                model =[field['v'] for field in transport['pflds'].values() if field['n'] == 'brand']
                reg_number = [field['v'] for field in transport['pflds'].values() if field['n'] == 'color']
                transport_props.append({
                    "id": transport['id'],
                    "name": transport['nm'],
                    'department': department[0] if department else None,
                    'model': model[0] if model else None,
                    'reg_number': reg_number[0] if reg_number else None,
                    'source': 'wialon'
                })
            add_wialon_transport_if_not_exists(transport_props)
            self.load_transport_in_memory(transports)
            await asyncio.sleep(discreteness)

    def load_transport_in_memory(self, transports):
        self.transport_map = {str(transport['id']):transport for transport in transports}
