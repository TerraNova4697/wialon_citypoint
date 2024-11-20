from abc import ABC, abstractmethod

from tb_gateway_mqtt import TBGatewayMqttClient

from monitoring_source.abs_transport_src import AbstractTransportSource
from mqtt_client.abs_destination import AbstractDestination

class AbstractConnector(ABC):
    def __init__(
            self,
            source: AbstractTransportSource,
            destination: AbstractDestination | None,
            data=None
    ):
        if data is None:
            data = {}
        self.source = source
        self.destination = destination
        self.data = data
        self.transport_map = {}

    @abstractmethod
    async def start_loop(self):
        pass

    @abstractmethod
    async def fetch_transport_states(self, discreteness: int):
        pass

    @abstractmethod
    def load_transport_in_memory(self, transports):
        pass
