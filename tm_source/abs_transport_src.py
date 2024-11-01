from abc import ABC, abstractmethod


class AbstractTransportSource(ABC):
    def __init__(self, login = None, client_id = None, secret_key = None, password = None, access = None):
        self.login = login
        self.client_id = client_id
        self.secret_key = secret_key
        self.password = password
        self.access = access

    @abstractmethod
    def get_transports(self, query_filter: str = ''):
        pass

    @abstractmethod
    def get_velocity_zones(self):
        pass

    @abstractmethod
    def get_planned_routes(self):
        pass

    @abstractmethod
    def get_incidents(self):
        pass

    @abstractmethod
    def auth(self):
        pass

    @abstractmethod
    def is_connected(self):
        pass

    @abstractmethod
    def update_token(self, token_params: dict):
        pass

    @abstractmethod
    def get_transport_list(self):
        pass

    @abstractmethod
    def get_messages(self):
        pass
