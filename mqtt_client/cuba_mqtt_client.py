from tb_gateway_mqtt import TBGatewayMqttClient


class CubaMqttClient:
    def __init__(self, mqtt_client: TBGatewayMqttClient):
        self.mqtt_client = mqtt_client

    def send_transport_data(self, transport_data):
        pass
