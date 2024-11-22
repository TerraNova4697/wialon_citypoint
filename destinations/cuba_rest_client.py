"""REST client for Cuba Core API"""
import os
import logging


from tb_rest_client import RestClientPE
from tb_rest_client.models.models_pe.device import Device
from tb_rest_client.rest import ApiException

from telemetry_objects.alarm import Alarm

logger = logging.getLogger(os.environ.get('LOGGER'))


class CubaRestClient:

    def __init__(self):
        self.BASE_URL = os.environ.get('CUBA_URL')
        self.CUBA_USER = os.environ.get('CUBA_USER')
        self.CUBA_PASSWORD = os.environ.get('CUBA_PASSWORD')

    def post_alarm(self, alarm: Alarm, device_name: str):
        """
        Form Alarm REST object and send it to the core. Return True if success, False otherwise
        :param alarm: Alarm
        :param device_name:
        :return:
        """
        with RestClientPE(base_url=self.BASE_URL) as rest_client:
            try:
                rest_client.login(self.CUBA_USER, self.CUBA_PASSWORD)

                device = rest_client.get_tenant_device(device_name=device_name)
                alarm = alarm.to_rest_object(device.id)
                rest_client.save_alarm(alarm)
                return True

            except ApiException as e:
                logger.exception(e)
                return False
            finally:
                try:
                    rest_client.logout()
                except ApiException as e:
                    logger.exception(e)

    def get_tenant_device(self, device_name: str) -> Device:
        """
        Get device by its name
        :param device_name:
        :return:
        """
        with RestClientPE(base_url=self.BASE_URL) as rest_client:
            try:
                rest_client.login(self.CUBA_USER, self.CUBA_PASSWORD)

                device = rest_client.get_tenant_device(device_name)
                return device

            except ApiException as e:
                logger.exception(e)
            finally:
                try:
                    rest_client.logout()
                except ApiException as e:
                    logger.exception(e)

    def get_transport_devices(self) -> list[Device]:
        """
        Get list of devices with device profile == KMG Transport
        :return: list of devices with profile == KMG Transport
        """
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
                try:
                    rest_client.logout()
                except ApiException as e:
                    logger.exception(e)
