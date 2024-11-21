from psycopg2.errors import UniqueViolation
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import exists
from sqlalchemy import select

from database.database import Session
from database.models import Sensor


class SensorORM:

    @staticmethod
    def get_all_sensors():
        with Session() as session:
            res = session.query(Sensor).all()
            return res

    @staticmethod
    def get_fuel_sensors_ids():
        with Session() as session:
            query = select(Sensor.id).where(Sensor.destination == 100)
            return session.execute(query).scalars().all()

    @staticmethod
    def get_sensors_by_destination(destination):
        with Session() as session:
            query = select(Sensor.id).where(Sensor.destination == destination)
            return session.execute(query).scalars().all()

    @staticmethod
    def add_sensors_if_not_exist(sensors):
        with Session() as session:
            for sensor in sensors:
                try:
                    if not session.query(exists().where(Sensor.id == int(sensor['id']))).scalar():
                        session.add(Sensor(
                            id=int(sensor['id']),
                            sensor_name=sensor['attributes']['SensorName'],
                            destination=sensor['attributes']['Destination'],
                            sensor_type=sensor['attributes']['SensorType'],
                        ))
                        session.commit()
                except (IntegrityError, UniqueViolation):
                    pass
