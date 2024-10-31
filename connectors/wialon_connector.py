import asyncio

from connectors.abs_connector import AbstractConnector
from telemetry_objects.transport import Transport

from database.operations import save_unsent_telemetry, add_wialon_transport_if_not_exists


class WialonConnector(AbstractConnector):

    async def start_loop(self):
        while not self.source.auth():
            print('Failed authentication')
            await asyncio.sleep(10)

        asyncio.create_task(self.check_transport_with_discreteness(86400))
        asyncio.create_task(self.fetch_transport_states(16))

    async def fetch_transport_states(self, discreteness: int):
        while True:
            data = self.source.get_transports()
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
                    if not self.destination.send_data(*t.form_mqtt_message()):
                        print("DATA NOT SENT")
                        # TODO: Save telemetry to DB.

            # if not self.destination or not self.destination.send_data(telemetry):
            #     print("DATA SENT WIALON")
            #     save_unsent_telemetry(telemetry)

            await asyncio.sleep(discreteness)

    async def check_transport_with_discreteness(self, discreteness: int):
        while True:
            transports_result = self.source.get_transport_list()
            transports = transports_result['items']
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
                    'reg_number': reg_number[0] if reg_number else None
                })
            add_wialon_transport_if_not_exists(transport_props)
            self.load_transport_in_memory(transports)
            await asyncio.sleep(discreteness)

    def load_transport_in_memory(self, transports):
        self.transport_map = {str(transport['id']):transport for transport in transports}
