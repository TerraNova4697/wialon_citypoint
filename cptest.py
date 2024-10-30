from tm_source.citypoint_source import CityPointSource
from time import sleep


cp = CityPointSource(
    login="KMG_Security",
    password="123456789KMG",
    secret_key="731kug8JEBaaHNf0rZI0LHID96dbglnWef8p8D8w",
    client_id="116"
)

res = cp.auth()
sleep(1)
cp.get_sensors()