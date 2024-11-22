import logging
import os

from database.database import Session
from database.models import CarState
from telemetry_objects.transport import Transport


logger = logging.getLogger(os.environ.get("LOGGER"))


class CarStateORM:

    @staticmethod
    def get_history_data(transport_id: int) -> list[CarState]:
        """
        Get CarState objects (limited with 30 elements) from DB in ASC
        :param transport_id: int
        :return: list of 30 CarState objects
        """
        with Session() as session:
            return session.query(CarState) \
                .filter(CarState.car_id == transport_id) \
                .order_by(CarState.ts.asc()) \
                .limit(30) \
                .all()

    @staticmethod
    def delete_car_states(data: list[CarState]):
        """
        Delete list of carStates.
        :param data: list of CarState objects
        :return:
        """
        with Session() as session:
            for car_state in data:
                session.delete(car_state)
            session.commit()

    @staticmethod
    def save_unsent_telemetry_list(telemetry: list[Transport]):
        """
        Save CarState objects
        :param telemetry: list of Transport objects
        :return:
        """
        logger.warning("Save unsent telemetry list")
        with Session() as session:
            for data in telemetry:
                session.add(CarState(
                    **data.to_model()
                ))
            session.commit()

    @staticmethod
    def save_unsent_telemetry(telemetry: Transport):
        """
        Save single CarState object
        :param telemetry: Transport object
        :return:
        """
        logger.warning(f"save unsent telemetry")
        with Session() as session:
            session.add(CarState(
                **telemetry.to_model()
            ))
            session.commit()
