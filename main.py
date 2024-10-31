import asyncio
import os
import logging

from dotenv import load_dotenv
from tb_gateway_mqtt import TBGatewayMqttClient

from config import config_log
from mqtt_client.cuba_mqtt_client import CubaMqttClient

load_dotenv()

from connectors.city_point_connector import CityPointConnector
from connectors.wialon_connector import WialonConnector
from database.database import db_init
from tm_source.citypoint_source import CityPointSource
from tm_source.wialon_source import WialonSource

logger = logging.getLogger(os.environ.get('LOGGER'))


async def main():
    mqtt_client = TBGatewayMqttClient(
        os.environ.get('CUBA_URL'),
        int(os.environ.get('CUBA_PORT')),
        os.environ.get("CUBA_GATEWAY_TOKEN"),
        client_id=os.environ.get("CUBA_CLIENT_ID")
    )
    mqtt_client.connect()
    if mqtt_client.is_connected():
        logger.info('Connected to Core')
    destination = CubaMqttClient(mqtt_client)

    cp_source = CityPointSource(
        login=os.environ.get("CITY_POINT_LOGIN"),
        password=os.environ.get("CITY_POINT_PASSWORD"),
        secret_key=os.environ.get("CITY_POINT_SECRET_KEY"),
        client_id=os.environ.get("CITY_POINT_CLIENT_ID")
    )
    cpc = CityPointConnector(cp_source, destination)

    wialon_source = WialonSource(
        secret_key=os.environ.get("WIALON_REFRESH_TOKEN")
    )
    wialon_connector = WialonConnector(wialon_source, destination)

    asyncio.create_task(wialon_connector.start_loop())
    asyncio.create_task(cpc.start_loop())

    logger.info('Integration is up and running')
    while True:
        await asyncio.sleep(10)


if __name__ == '__main__':
    config_log()
    db_init()
    asyncio.run(main())

