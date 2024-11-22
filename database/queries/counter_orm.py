from sqlalchemy import select, and_, func
from sqlalchemy.sql import exists

from database.database import Session
from database.models import Counter, Car


class CounterORM:

    @staticmethod
    def save_counter(mileage: int, engine_seconds: int, ts: int, car_id: int):
        """
        Save current state of transport counter
        :param mileage: Current total mileage
        :param engine_seconds: Current total engine working seconds
        :param ts: Timestamp of the record
        :param car_id: ID of the transport
        :return:
        """
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

    @staticmethod
    def get_counters_for_period(start_ts: int, end_ts: int) -> list[Counter]:
        """
        Get list of Counter objects for the given period grouped by its car_id
        :param start_ts: Start period as timestamp
        :param end_ts: End period as timestamp
        :return:
        """
        with (Session as session):
            return session.query(Counter) \
                .where(Counter.ts >= start_ts) \
                .where(Counter.ts <= end_ts) \
                .group_by(Counter.car_id) \
                .all()

    @staticmethod
    def add_counter(mileage: int, engine_seconds: int, ts: int, car_id: int):
        """
        Save current state of transport counter
        :param mileage: Current total mileage
        :param engine_seconds: Current total engine working seconds
        :param ts: Timestamp of the record
        :param car_id: ID of the transport
        :return:
        """
        with Session() as session:
            session.add(Counter(mileage=mileage, engine_seconds=engine_seconds,
                                ts=ts, car_id=car_id))
            session.commit()

    @staticmethod
    def get_day_stats(start_ts: int, end_ts: int):
        """
        Get mileage end engine working seconds for the given period
        :param start_ts: Start period as timestamp
        :param end_ts: End period as timestamp
        :return:
        """
        with Session() as session:
            subquery = (
                select(
                    func.min(Counter.mileage).label('mileage_min'),
                    func.max(Counter.mileage).label('mileage_max'),
                    func.min(Counter.engine_seconds).label('engine_seconds_min'),
                    func.max(Counter.engine_seconds).label('engine_seconds_max'),
                    Counter.car_id
                )
                .where(
                    and_(
                        Counter.ts >= start_ts,
                        Counter.ts < end_ts
                    )
                )
                .group_by(Counter.car_id)
                .subquery('subquery')
            )
            query = (
                select(
                    (subquery.c.mileage_max - subquery.c.mileage_min).label('mileage'),
                    (subquery.c.engine_seconds_max - subquery.c.engine_seconds_min).label('engine_seconds'),
                    subquery.c.car_id
                )
            )
            return session.execute(query).all()
