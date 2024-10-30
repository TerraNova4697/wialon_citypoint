from abc import ABC, abstractmethod


class AbstractDestination(ABC):

    @abstractmethod
    def send_data(self, data) -> bool:
        pass