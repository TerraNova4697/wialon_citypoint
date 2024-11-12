from itertools import chain
import logging
import os
import re

from sqlalchemy.exc import IntegrityError, MultipleResultsFound, NoResultFound
from psycopg2.errors import UniqueViolation
from sqlalchemy.sql import exists

from database.database import Session, engine
from database.models import Sensor, Car, CarState, RunTime, Counter
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


def get_history_data(transport_id):
    with Session() as session:
        return session.query(CarState).filter(CarState.car_id == transport_id).order_by(CarState.ts.asc()).limit(30).all()


def get_transport_ids(source = None):
    with Session() as session:
        query = session.query(Car.id)
        if source:
            query = query.where(Car.source == source)
            print(query)
        return list(chain(*query.all()))


def delete_car_states(data):
    with Session() as session:
        for car_state in data:
            session.delete(car_state)
        session.commit()


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


def save_counter(mileage, engine_seconds, ts, car_id):
    if mileage is None and engine_seconds is None:
        return
    with Session() as session:
        if session.query(exists().where(Car.id == car_id)).scalar():
            session.add(Counter(
                mileage=mileage,
                engine_seconds=engine_seconds,
                ts=ts,
                car_id=car_id
            ))
            session.commit()


def get_counters_for_period(car_id, start_ts, end_ts):
    with (Session as session):
        session.query(Counter) \
            .where(Counter.ts >= start_ts) \
            .where(Counter.ts <= end_ts) \
            .group_by(Counter.car_id) \
            .all()


def add_counter(mileage, engine_seconds, ts, car_id):
    with Session() as session:
        session.add(Counter(mileage=mileage, engine_seconds=engine_seconds,
                            ts=ts, car_id=car_id))
        session.commit()


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
                    reg_number = re.sub('[_\-|\s]', '', transport.get('attributes', {}).get('RegNumber', ''))
                    session.add(Car(
                        id=int(transport['id']),
                        name=reg_number,
                        model=model,
                        reg_number=reg_number,
                        is_hidden=transport.get('attributes', {}).get('IsHidden'),
                        source='city_point'
                    ))
                    session.commit()
            except (IntegrityError, UniqueViolation) as e:
                pass


def get_all_transport_names():
    with Session() as session:
        return session.query(Car.id, Car.name).all()


def save_unsent_telemetry_list(telemetry: list[Transport]):
    logger.warning("Save unsent telemetry list")
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
    logger.warning(f"save unsent telemetry")
    with Session() as session:
        session.add(CarState(
            **telemetry.to_model()
        ))
        session.commit()


def create_runtime(start_ts, end_ts):
    with Session() as session:
        session.add(RunTime(start_ts=start_ts, end_ts=end_ts))
        session.commit()
