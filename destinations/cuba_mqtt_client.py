"""Class that forms """
import asyncio
import os
import logging

from tb_device_mqtt import TBPublishInfo
from tb_gateway_mqtt import TBGatewayMqttClient

from database.queries import CarORM, CarStateORM
from destinations.abs_destination import AbstractDestination
from telemetry_objects.transport import Transport

logger = logging.getLogger(os.environ.get('LOGGER'))


class CubaMqttClient(AbstractDestination):

    def __init__(self, mqtt_client: TBGatewayMqttClient):
        self.mqtt_client = mqtt_client
        self.transport_map = {transport[0]: transport[1] for transport in CarORM.get_all_transport_names()}

    def send_data(self, device_name: str, telemetry: dict | list) -> bool:
        """
        Send a single telemetry for given device name
        :param device_name:
        :param telemetry:
        :return:
        """
        result = self.mqtt_client.gw_send_telemetry(device_name, telemetry)
        successful = result.rc() == TBPublishInfo.TB_ERR_SUCCESS
        if not successful:
            logger.warning(f"Telemetry was not sent: {device_name}, {telemetry}")
        return result.rc() == TBPublishInfo.TB_ERR_SUCCESS

    async def send_history_data(self):
        """
        Get all historical data for every transport and send it to the core as telemetry
        :return:
        """
        transport_ids = CarORM.get_transport_ids()
        for transport_id in transport_ids:
            device_name = self.transport_map[transport_id]
            while data := CarStateORM.get_history_data(transport_id):
                telemetry = [
                    Transport.model_to_mqtt_message(self.transport_map[state.car_id], state)[1] for state in data
                ]
                result = self.mqtt_client.gw_send_telemetry(device_name, telemetry)
                print(result.rc())
                if result.rc() == TBPublishInfo.TB_ERR_SUCCESS:
                    CarStateORM.delete_car_states(data)
                await asyncio.sleep(0.1)
        await asyncio.sleep(10)
