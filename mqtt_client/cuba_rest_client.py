import os
import logging


from tb_rest_client import RestClientPE
from tb_rest_client.models.models_pe import Alarm, DeviceId
from tb_rest_client.rest import ApiException


logger = logging.getLogger(os.environ.get('LOGGER'))


class CubaRestClient:

    def __init__(self):
        self.BASE_URL = os.environ.get('CUBA_URL')
        self.CUBA_USER = os.environ.get('CUBA_USER')
        self.CUBA_PASSWORD = os.environ.get('CUBA_PASSWORD')

    def post_alarm(self, alarm, device_name):
        with RestClientPE(base_url=self.BASE_URL) as rest_client:
            try:
                rest_client.login(self.CUBA_USER, self.CUBA_PASSWORD)
                # get device by name
                device = rest_client.get_tenant_device(device_name)
                alarm = alarm.to_rest_object()
                # Set device id as attribute
                alarm.device = device.id
                rest_client.save_alarm(alarm)
                return True

            except ApiException as e:
                logger.exception(e)
                return False
            finally:
                rest_client.logout()

    def get_tenant_device(self, device_name):
        with RestClientPE(base_url=self.BASE_URL) as rest_client:
            try:
                rest_client.login(self.CUBA_USER, self.CUBA_PASSWORD)
                device = rest_client.get_tenant_device(device_name)
                return device

            except ApiException as e:
                logger.exception(e)
            finally:
                rest_client.logout()

    def get_transport_devices(self):
        with RestClientPE(base_url=self.BASE_URL) as rest_client:
            try:
                rest_client.login(self.CUBA_USER, self.CUBA_PASSWORD)
                devices = rest_client.get_tenant_devices(
                    500, 0, 'KMG Transport'
                )
                return devices.data

            except ApiException as e:
                logger.exception(e)
            finally:
                rest_client.logout()
