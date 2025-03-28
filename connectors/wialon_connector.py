import asyncio
import json
import os
import logging
import re
from http.client import RemoteDisconnected
from datetime import datetime, timedelta

from urllib3.exceptions import NameResolutionError
from requests.exceptions import ConnectionError as RequestsConnectionError

from connectors.abs_connector import AbstractConnector
from database.queries import CarORM, CounterORM, CarStateORM
from destinations.abs_destination import AbstractDestination
from destinations.cuba_rest_client import CubaRestClient
from monitoring_source.wialon_source import WialonSource
from telemetry_objects.alarm import Alarm
from telemetry_objects.transport import Transport


logger = logging.getLogger(os.environ.get("LOGGER"))


class WialonConnector(AbstractConnector):

    def __init__(
            self,
            source: WialonSource,
            destination: AbstractDestination | None,
            data=None,
            rest_client: CubaRestClient = None
    ):
        super().__init__(source, destination, data)
        self.rest_client: CubaRestClient | None = rest_client

    async def start_loop(self):
        try:
            res = self.source.auth()
        except (RequestsConnectionError, NameResolutionError, TimeoutError, RemoteDisconnected) as exc:
            logger.exception(f"Exception trying to authenticate: {exc}")
            res = False
        while not res:
            logger.info('Failed authentication')
            await asyncio.sleep(10)
            await self.start_loop()

        wialon_transport_ids = CarORM.get_transport_ids('wialon')
        asyncio.create_task(self.check_transport_with_discreteness(86400))
        # if runtime := get_last_runtime():
        #     asyncio.create_task(self.get_states_since(runtime))
        self.source.manage_session_units(wialon_transport_ids)
        asyncio.create_task(self.get_avls(2))
        asyncio.create_task(self.daily_report(hour=6, minute=0))
        asyncio.create_task(self.monitor_counters(600))
        # asyncio.create_task(self.send_day_report())

    async def daily_report(self, hour, minute):
        while True:
            # Get the current time
            now = datetime.now()
            next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

            await self.send_report()

            if next_run < now:
                next_run += timedelta(days=1)

            await self.send_report()

            time_to_wait = (next_run - now).total_seconds()
            logger.info(f"Waiting {time_to_wait} seconds until the next run at {next_run}")

            await asyncio.sleep(time_to_wait)

            await self.send_report()

    def check_drivers_with_discreteness(self, discreteness):
        pass

    async def send_report(self):
        dt = datetime.today().replace(hour=6, minute=0, second=0, microsecond=0)
        start_ts = int(datetime.timestamp(dt.replace(hour=0) - timedelta(days=1)))
        end_ts = int(datetime.timestamp(dt.replace(hour=0)))
        res = CounterORM.get_day_stats(start_ts, end_ts)
        for record in res:
            mileage, engine_hours, car_id = record
            car = CarORM.get_car_by_id(car_id)
            if not car:
                continue

            try:
                self.destination.send_data(
                    car.name,
                    {
                        'ts': int(round(datetime.timestamp(dt - timedelta(days=1)) * 1000)),
                        'values': {
                            'mileage': mileage,
                            'working_hours': engine_hours
                        }
                    }
                )
            except Exception as exc:
                logger.exception(exc)

    async def monitor_counters(self, discreteness):
        while True:
            try:
                data = self.source.get_counters_info()
            except (RequestsConnectionError, NameResolutionError, TimeoutError, RemoteDisconnected) as exc:
                logger.exception(f"Exception trying to fetch counters: {exc}")
                await asyncio.sleep(10)
                continue

            if data.get('items'):
                ts = datetime.timestamp(datetime.now())
                for item in data['items']:
                    CounterORM.save_counter(
                        mileage=item.get('cnm'),
                        engine_seconds=int(item['cneh'] * 3600) if item.get('cneh') else None,
                        ts=ts,
                        car_id=item['id']
                    )

            await asyncio.sleep(discreteness)

    async def get_states_since(self, runtime):
        start_ts, end_ts = runtime.end_ts, int(datetime.timestamp(datetime.now()))

        try:
            transport_ids = CarORM.get_transport_ids('wialon')

            for transport_id in transport_ids:
                res = self.source.load_historical_messages_by_id(transport_id, start_ts, end_ts)
                self.source.unload()
                self.save_trips(transport_id, res.get('messages', []))
                await asyncio.sleep(10)

        except Exception as exc:
            logger.exception(exc)

    @staticmethod
    def save_trips(transport_id, trips):
        car = CarORM.get_car_by_id(transport_id)
        for trip in trips:
            if trip['pos']['s'] > 3:
                CarStateORM.save_unsent_telemetry(Transport(
                    ts=trip['t'],
                    is_sent=False,
                    latitude=trip['pos']['y'],
                    longitude=trip['pos']['x'],
                    velocity=trip['pos']['s'],
                    fuel_level=None,
                    car_id=transport_id,
                    ignition=trip['p'].get('io_239'),
                    light=None,
                    last_conn=trip['rt'],
                    name=car.name
                ))

    async def get_avls(self, discreteness: int):
        while True:
            logger.info('Fetching states')
            try:
                data = self.source.get_avl_event()
            except (RequestsConnectionError, NameResolutionError, TimeoutError, RemoteDisconnected) as exc:
                self.source.reinitialize_session(CarORM.get_transport_ids('wialon'))
                logger.exception(f"Exception trying to fetch transport states: {exc}")
                await asyncio.sleep(10)
                continue

            if not data:
                continue
            for event in data.get('events', []):
                try:
                    if event['d'].get('tp') == 'ud':
                        await self.parse_transport_state(event)
                    elif event['d'].get('tp') == 'evt':
                        await self.parse_violation(event)
                except KeyError as e:
                    logger.exception(event)
                    logger.exception(e)

            await asyncio.sleep(discreteness)

    async def parse_violation(self, event):
        if event['d']['f'] == 1537:
            logger.warning(event['d']['p']['task_evt_name'])
            logger.warning(event)
            a = Alarm(
                id=event['i'],
                title=event['d']['p']['task_evt_name'],
                message=event['d']['et'],
                level=7,
                latitude=event['d']['y'],
                longitude=event['d']['x'],
                record_date=event['d']['t'],
                date_of_creation=event['d']['p']['task_update_time'],
                car_id=event['i'],
                place=json.loads(event['d']['p'].get('task_tags', '')).get('ZONE', '')
            )
            car = CarORM.get_car_by_id(a.car_id)
            logger.warning(f"{a}, {car.name}")
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
                name=CarORM.get_car_by_id(event['i']).name
            )
            if not self.destination or not self.destination.send_data(*t.form_mqtt_message()):
                CarStateORM.save_unsent_telemetry(t)

    # async def send_day_report(self, hour=6, minute=0, second=0):
    #     start_ts = int(datetime.timestamp((datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0)))
    #     end_ts = int(datetime.timestamp((datetime.now() - timedelta(days=1)).replace(hour=23, minute=59, second=59)))
    #
    #     counters = CounterORM.get_counters_for_period(start_ts, end_ts)

    async def fetch_transport_states(self, discreteness: int):
        while True:
            logger.info('Fetching states')
            try:
                data = self.source.get_transports()
            except (RequestsConnectionError, NameResolutionError, TimeoutError, RemoteDisconnected) as exc:
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
                        CarStateORM.save_unsent_telemetry(t)

            await asyncio.sleep(discreteness)

    async def fetch_notifications(self, discreteness):
        while True:
            try:
                events = self.source.get_messages()
                logger.info(json.dumps(events))
            except (RequestsConnectionError, NameResolutionError, TimeoutError, RemoteDisconnected) as exc:
                logger.exception(f"Exception trying to fetch updates: {exc}")
                await asyncio.sleep(10)
                continue

            await asyncio.sleep(discreteness)

    async def check_transport_with_discreteness(self, discreteness: int):
        while True:
            try:
                transports_result = self.source.get_transport_list()
            except (RequestsConnectionError, NameResolutionError, TimeoutError, RemoteDisconnected) as exc:
                logger.exception(f"Exception trying to fetch transport list: {exc}")
                await asyncio.sleep(10)
                continue
            transports = transports_result.get('items', [])
            logger.info(f"Fetched {len(transports)} units")
            transport_props = []
            mobile_group_regex = re.compile(r'[мМM][гГ]\s?-\s?\d{1,4}\D')
            for transport in transports:

                department = [field['v'] for field in transport['pflds'].values() if field['n'] == 'vehicle_type']
                model =[field['v'] for field in transport['pflds'].values() if field['n'] == 'brand']
                reg_number = [field['v'] for field in transport['pflds'].values() if field['n'] == 'color']

                if len(reg_number) == 0:
                    continue

                reg_number = re.sub('[_\-|\s]', '', reg_number[0])

                mobile_group_matches = re.findall(mobile_group_regex, transport['nm'])
                if mobile_group_matches:
                    mobile_group = mobile_group_matches[0].replace(' ', '')
                    name = f'{mobile_group} {reg_number}'
                else:
                    name = reg_number

                transport_props.append({
                    "id": transport['id'],
                    "name": name,
                    'department': department[0] if department else None,
                    'model': model[0] if model else None,
                    'reg_number': reg_number,
                    'source': 'wialon'
                })
            CarORM.add_wialon_transport_if_not_exists(transport_props)
            self.load_transport_in_memory(transports)
            await asyncio.sleep(discreteness)

    def load_transport_in_memory(self, transports):
        self.transport_map = {str(transport['id']):transport for transport in transports}
