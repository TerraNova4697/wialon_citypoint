import asyncio
import os

from dotenv import load_dotenv
load_dotenv()

from connectors.city_point_connector import CityPointConnector
from connectors.wialon_connector import WialonConnector
from database.database import db_init
from tm_source.citypoint_source import CityPointSource
from tm_source.wialon_source import WialonSource


async def main():
    cp_source = CityPointSource(
        login=os.environ.get("CITY_POINT_LOGIN"),
        password=os.environ.get("CITY_POINT_PASSWORD"),
        secret_key=os.environ.get("CITY_POINT_SECRET_KEY"),
        client_id=os.environ.get("CITY_POINT_CLIENT_ID")
    )
    cpc = CityPointConnector(cp_source, None)

    wialon_source = WialonSource(
        secret_key=os.environ.get("WIALON_REFRESH_TOKEN")
    )
    wialon_connector = WialonConnector(wialon_source, None)

    asyncio.create_task(wialon_connector.start_loop())
    asyncio.create_task(cpc.start_loop())
    while True:
        await asyncio.sleep(10)


if __name__ == '__main__':
    db_init()
    asyncio.run(main())

