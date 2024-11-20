import asyncio
import os
import logging
from datetime import datetime

from dotenv import load_dotenv
from tb_gateway_mqtt import TBGatewayMqttClient

from config import config_log
from database.operations import create_runtime
from destinations.cuba_mqtt_client import CubaMqttClient
from destinations.cuba_rest_client import CubaRestClient

load_dotenv()

from connectors.city_point_connector import CityPointConnector
from connectors.wialon_connector import WialonConnector
from database.database import db_init
from monitoring_source.citypoint_source import CityPointSource
from monitoring_source.wialon_source import WialonSource

logger = logging.getLogger(os.environ.get('LOGGER'))


async def main():
    mqtt_client = TBGatewayMqttClient(
        os.environ.get('CUBA_MQTT_HOST'),
        int(os.environ.get('CUBA_PORT')),
        os.environ.get("CUBA_GATEWAY_TOKEN"),
        client_id=os.environ.get("CUBA_CLIENT_ID")
    )
    mqtt_client.connect()
    if mqtt_client.is_connected():
        logger.info('Connected to Core')

    destination = CubaMqttClient(mqtt_client)
    asyncio.create_task(destination.send_history_data())
    rest_client = CubaRestClient()

    cp_source = CityPointSource(
        login=os.environ.get("CITY_POINT_LOGIN"),
        password=os.environ.get("CITY_POINT_PASSWORD"),
        secret_key=os.environ.get("CITY_POINT_SECRET_KEY"),
        client_id=os.environ.get("CITY_POINT_CLIENT_ID")
    )
    cpc = CityPointConnector(
        source=cp_source,
        destination=destination,
        rest_client=rest_client
    )

    wialon_source = WialonSource(
        secret_key=os.environ.get("WIALON_REFRESH_TOKEN")
    )
    wialon_connector = WialonConnector(
        source=wialon_source,
        destination=destination,
        rest_client=rest_client
    )

    asyncio.create_task(wialon_connector.start_loop())
    asyncio.create_task(cpc.start_loop())

    logger.info('Integration is up and running')
    while True:
        await asyncio.sleep(10)


if __name__ == '__main__':
    config_log()
    db_init()

    start_ts = datetime.timestamp(datetime.now())
    try:
        asyncio.run(main())
    finally:
        end_ts = datetime.timestamp(datetime.now())
        create_runtime(start_ts=start_ts, end_ts=end_ts)


