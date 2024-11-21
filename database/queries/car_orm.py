import re

from psycopg2.errors import UniqueViolation
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import exists

from database.database import Session
from database.models import Car


class CarORM:

    @staticmethod
    def get_transport_ids(source=None):
        with Session() as session:
            query = session.query(Car.id)
            if source:
                query = query.where(Car.source == source)
            return session.execute(query).scalars().all()

    @staticmethod
    def get_all_cars_ids():
        with Session() as session:
            query = select(Car.id).where(Car.is_hidden == False)
            return session.execute(query).scalars().all()

    @staticmethod
    def get_car_by_id(car_id):
        with Session() as session:
            return session.query(Car).where(Car.id == car_id).scalar()

    @staticmethod
    def get_all_cars():
        with Session() as session:
            return session.query(Car.name, Car.department, Car.model).where(Car.source == 'wialon').all()

    @staticmethod
    def add_wialon_transport_if_not_exists(transports):
        with Session() as session:
            for transport in transports:
                try:
                    if not session.query(exists().where(Car.id == int(transport['id']))).scalar():
                        session.add(Car(**transport))
                        session.commit()
                except (IntegrityError, UniqueViolation):
                    pass

    @staticmethod
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
                except (IntegrityError, UniqueViolation):
                    pass

    @staticmethod
    def get_all_transport_names():
        with Session() as session:
            return session.query(Car.id, Car.name).all()
