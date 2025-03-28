from database.models import CarState


class Transport:
    def __init__(self, ts, is_sent, latitude, longitude, velocity, fuel_level, car_id, ignition, light, last_conn, name):
        self.ts = int(ts)
        self.is_sent = is_sent
        self.latitude = latitude
        self.longitude = longitude
        self.velocity = velocity
        self.fuel_level = fuel_level if fuel_level and fuel_level > 0 else None
        self.car_id = car_id
        self.ignition = ignition
        self.light = light
        self.last_conn = int(last_conn)
        self.name = name

    def __repr__(self):
        return f"({self.latitude}, {self.longitude}) Velocity: {self.velocity}, Fuel(l): {self.fuel_level}"

    def to_model(self):
        return {
            "ts": self.ts,
            "is_sent": self.is_sent,
            "lat": self.latitude,
            "lon": self.longitude,
            "velocity": self.velocity,
            "car_id": self.car_id,
            "fuel_level": self.fuel_level,
            'ignition': self.ignition,
            'light': self.light,
            'last_conn': self.last_conn
        }

    def __dict__(self):
        data = {
            'latitude': self.latitude,
            'longitude': self.longitude,
            'last_conn': int(round(self.last_conn * 1000)),
            'velocity': self.velocity if self.velocity else 0
        }
        if self.fuel_level:
            data['fuel_level'] = self.fuel_level
        if self.light:
            data['light'] = self.light
        if self.ignition:
            data['ignition'] = self.ignition
        return {
            'ts': int(round(self.ts * 1000)),
            'values': data
        }

    @staticmethod
    def model_to_mqtt_message(car_name, car_state: CarState) -> tuple:
        return car_name, Transport(
            ts=car_state.ts,
            is_sent=False,
            latitude=car_state.lat,
            longitude=car_state.lon,
            velocity=car_state.velocity,
            fuel_level=car_state.fuel_level,
            car_id=car_state.car_id,
            ignition=car_state.ignition,
            light=car_state.light,
            last_conn=car_state.last_conn,
            name=car_name
        ).__dict__()

    def form_mqtt_message(self) -> tuple:
        return self.name, self.__dict__()
