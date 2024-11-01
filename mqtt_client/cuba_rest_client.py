import os

from sqlalchemy.testing.plugin.plugin_base import logging
from tb_rest_client import RestClientPE
from tb_rest_client.models.models_pe import Alarm, DeviceId
from tb_rest_client.rest import ApiException


logger = logging.getLogger(os.environ.get('LOGGER'))


class CubaRestClient:

    def __init__(self):
        self.BASE_URL = os.environ.get('CUBA_URL')
        self.CUBA_USER = os.environ.get('CUBA_USER')
        self.CUBA_PASSWORD = os.environ.get('CUBA_PASSWORD')

    def post_alarm(self, alarm):
        with RestClientPE(base_url=self.BASE_URL) as rest_client:
            try:
                rest_client.login(self.CUBA_USER, self.CUBA_PASSWORD)
                # device_id = get_device_id_by_name(alarm.car_id)
                alarm = alarm.to_rest_object()
                alarm.device = DeviceId(
                    '80c4c6b0-9742-11ef-87ce-23643fc703ee',
                    'DEVICE'
                )
                rest_client.save_alarm(alarm)
                return True

            except ApiException as e:
                logging.exception(e)
                return False
            finally:
                rest_client.logout()
