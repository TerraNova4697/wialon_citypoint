import os
import logging

from tb_device_mqtt import TBPublishInfo
from tb_gateway_mqtt import TBGatewayMqttClient

from mqtt_client.abs_destination import AbstractDestination


logger = logging.getLogger(os.environ.get('LOGGER'))


class CubaMqttClient(AbstractDestination):

    def __init__(self, mqtt_client: TBGatewayMqttClient):
        self.mqtt_client = mqtt_client

    def send_data(self, device_name, telemetry) -> bool:
        result = self.mqtt_client.gw_send_telemetry(device_name, telemetry)
        successful = result.rc() == TBPublishInfo.TB_ERR_SUCCESS
        if not successful:
            logger.warning(f"Telemetry was not sent: {device_name}, {telemetry}")
        return result.rc() == TBPublishInfo.TB_ERR_SUCCESS
