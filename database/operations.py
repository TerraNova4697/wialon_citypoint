from itertools import chain
import logging
import os

from sqlalchemy.exc import IntegrityError, MultipleResultsFound, NoResultFound
from psycopg2.errors import UniqueViolation
from sqlalchemy.sql import exists

from database.database import Session
from database.models import Sensor, Car, CarState, RunTime
from telemetry_objects.transport import Transport


logger = logging.getLogger(os.environ.get("LOGGER"))


def get_all_sensors():
    with Session() as session:
        res = session.query(Sensor).all()
        return res


def get_fuel_sensors_ids():
    with Session() as session:
        res = session.query(Sensor.id).where(Sensor.destination == 100).all()
        return list(chain(*res))


def get_transport_ids(source):
    with Session() as session:
        return list(chain(*session.query(Car.id).where(Car.source == source).all()))


def get_sensors_by_destination(destination):
    with Session() as session:
        res = session.query(Sensor.id).where(Sensor.destination == destination).all()
        return list(chain(*res))


def get_all_cars_ids():
    with Session() as session:
        res = session.query(Car.id).where(Car.is_hidden == False).all()
        return list(chain(*res))


def get_car_by_id(car_id):
    with Session() as session:
        res = session.query(Car).where(Car.id == car_id).scalar()
        return res


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
            except (IntegrityError, UniqueViolation) as e:
                pass


def add_wialon_transport_if_not_exists(transports):
    with Session() as session:
        for transport in transports:
            try:
                if not session.query(exists().where(Car.id == int(transport['id']))).scalar():
                    session.add(Car(**transport))
                    session.commit()
            except (IntegrityError, UniqueViolation) as e:
                pass


def add_transport_if_not_exists(transports):
    with Session() as session:
        for transport in transports:
            try:
                if not session.query(exists().where(Car.id == int(transport['id']))).scalar():
                    model = transport.get('attributes', {}).get('Model', '')
                    reg_number = transport.get('attributes', {}).get('RegNumber', '').replace('_', ' ')
                    session.add(Car(
                        id=int(transport['id']),
                        name=f"{reg_number} {model}",
                        model=model,
                        reg_number=reg_number,
                        is_hidden=transport.get('attributes', {}).get('IsHidden'),
                        source='city_point'
                    ))
                    session.commit()
            except (IntegrityError, UniqueViolation) as e:
                pass

def save_unsent_telemetry_list(telemetry: list[Transport]):
    with Session() as session:
        for data in telemetry:
            session.add(CarState(
                **data.to_model()
            ))
        session.commit()


def get_last_runtime():
    with Session() as session:
        try:
            return session.query(RunTime).order_by(RunTime.id.desc()).first()
        except MultipleResultsFound as exc:
            logger.exception(exc)
            return None
        except NoResultFound as exc:
            logger.exception(exc)
            return None


def save_unsent_telemetry(telemetry: Transport):
    with Session() as session:
        session.add(CarState(
            **telemetry.to_model()
        ))
        session.commit()


def create_runtime(start_ts, end_ts):
    with Session() as session:
        session.add(RunTime(start_ts=start_ts, end_ts=end_ts))
        session.commit()
