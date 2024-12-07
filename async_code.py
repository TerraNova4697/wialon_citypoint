import asyncio
import json
import itertools
import os
import re

from connectors.city_point_connector import CityPointConnector
from connectors.wialon_connector import WialonConnector
from database.queries import CarORM
from monitoring_source.citypoint_asource import CityPointAsyncSource
from monitoring_source.citypoint_source import CityPointSource
from monitoring_source.wialon_source import WialonSource


async def run_code():
    # cp_source = CityPointSource(
    #     login=os.environ.get("CITY_POINT_LOGIN"),
    #     password=os.environ.get("CITY_POINT_PASSWORD"),
    #     secret_key=os.environ.get("CITY_POINT_SECRET_KEY"),
    #     client_id=os.environ.get("CITY_POINT_CLIENT_ID")
    # )
    # res = cp_source.auth()
    # print(res)
    wialon_source = WialonSource(
        secret_key=os.environ.get("WIALON_REFRESH_TOKEN")
    )
    # print(wialon_source.BASE_URL)
    wialon_source.auth()
    res = wialon_source.get_transports()
    # print(res)
    regex = re.compile(r'[мМM][гГ]\s?-\s?\d{1,4}\D')
    transport_props = []
    for transport in res['items']:
        department = [field['v'] for field in transport['pflds'].values() if field['n'] == 'vehicle_type']
        model = [field['v'] for field in transport['pflds'].values() if field['n'] == 'brand']
        reg_number = [field['v'] for field in transport['pflds'].values() if field['n'] == 'color']

        if len(reg_number) == 0:
            continue

        reg_number = re.sub('[_\-|\s]', '', reg_number[0])

    #     transport_props.append({
    #         "id": transport['id'],
    #         "name": reg_number,
    #         'department': department[0] if department else None,
    #         'model': model[0] if model else None,
    #         'reg_number': reg_number,
    #         'source': 'wialon'
    #     })
    # CarORM.add_wialon_transport_if_not_exists(transport_props)

        mobile_group_matches = re.findall(regex, transport['nm'])
        if mobile_group_matches:
            mobile_group = mobile_group_matches[0].replace(' ', '')
            name = f'{mobile_group} {reg_number}'
            CarORM.update_car_name(transport['id'], name)
        else:
            name = reg_number

    # with open("drivers_response.json", "w") as outfile:
    #     json_object = json.dumps(res, indent=4)
    #     outfile.write(json_object)
    # print(res)
    # await cpc.fetch_transport_states(60)

    # with (open("drivers_response.json", "r") as source):
    #     drivers_response = json.loads(source.read())
    #     drivers_lists = [list(field.get('drvrs').values()) for field in drivers_response['items'] if field.get('drvrs')]
    #     drivers = list(itertools.chain(*list(drivers_lists)))
    #
    #     actual_drivers = [driver for driver in drivers if driver['ds'].strip() == 'Driver']
    #
    #     print(len(actual_drivers))
    #
    #     for driver in actual_drivers:
    #         print(driver)
    #         print()
    #         print()


# if __name__ == '__main__':
#     asyncio.run(main())
