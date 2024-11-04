import asyncio
import os
import logging

from urllib3.exceptions import NameResolutionError

from connectors.abs_connector import AbstractConnector
from telemetry_objects.transport import Transport

from database.operations import save_unsent_telemetry, add_wialon_transport_if_not_exists


logger = logging.getLogger(os.environ.get("LOGGER"))


class WialonConnector(AbstractConnector):

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

        asyncio.create_task(self.check_transport_with_discreteness(86400))
        asyncio.create_task(self.fetch_transport_states(16))

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
