import re

from psycopg2.errors import UniqueViolation
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import exists

from database.database import Session
from database.models import Car


class CarORM:
    """__tablename__ = 'cars'"""

    @staticmethod
    def get_transport_ids(source: str | None=None) -> list[int]:
        """
        Get all transport IDs.
        :param source: query parameter for 'source' column.
        :return: list of IDs.
        """
        with Session() as session:
            query = session.query(Car.id)
            if source:
                query = query.where(Car.source == source)
            return session.execute(query).scalars().all()

    @staticmethod
    def update_car_name(car_id: str | int, name):
        with Session() as session:
            car = session.query(Car).where(Car.id == car_id).scalar()
            car.name = name
            session.add(car)
            session.commit()

    @staticmethod
    def get_all_cars_ids() -> list[int]:
        """
        Get all transport IDs.
        :return: list of IDs.
        """
        with Session() as session:
            query = select(Car.id).where(Car.is_hidden == False)
            return session.execute(query).scalars().all()

    @staticmethod
    def get_car_by_id(car_id: int) -> Car:
        """
        Get car by its ID.
        :param car_id: Car.id
        :return: Car
        """
        with Session() as session:
            return session.query(Car).where(Car.id == car_id).scalar()

    @staticmethod
    def get_all_cars() -> list[Car]:
        """
        Get cars
        :return: list of cars with columns name, department and model
        """
        with Session() as session:
            return session.query(Car.name, Car.department, Car.model).where(Car.source == 'wialon').all()

    @staticmethod
    def add_wialon_transport_if_not_exists(transports: list[dict]):
        """
        Add transport with source == 'wialon' if not exists in DB already
        :param transports: dictionary representation of transport
        :return:
        """
        with Session() as session:
            for transport in transports:
                try:
                    if not session.query(exists().where(Car.id == int(transport['id']))).scalar():
                        session.add(Car(**transport))
                        session.commit()
                except (IntegrityError, UniqueViolation):
                    pass

    @staticmethod
    def add_transport_if_not_exists(transports: list[dict]):
        """
        Add transport if not exists in DB already
        :param transports: dictionary representation of transport
        :return:
        """
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
                except (IntegrityError, UniqueViolation):
                    pass

    @staticmethod
    def get_all_transport_names() -> list[tuple]:
        """
        Get cars with columns id and name
        :return: list of Car objects
        """
        with Session() as session:
            return session.query(Car.id, Car.name).all()
