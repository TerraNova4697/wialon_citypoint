"""Abstract class of destination classes"""
from abc import ABC, abstractmethod


class AbstractDestination(ABC):

    @abstractmethod
    def send_data(self, device_name, telemetry) -> bool:
        pass