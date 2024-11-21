from sqlalchemy import select, and_, func
from sqlalchemy.sql import exists

from database.database import Session
from database.models import Counter, Car


class CounterORM:

    @staticmethod
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

    @staticmethod
    def get_counters_for_period(start_ts, end_ts):
        with (Session as session):
            session.query(Counter) \
                .where(Counter.ts >= start_ts) \
                .where(Counter.ts <= end_ts) \
                .group_by(Counter.car_id) \
                .all()

    @staticmethod
    def add_counter(mileage, engine_seconds, ts, car_id):
        with Session() as session:
            session.add(Counter(mileage=mileage, engine_seconds=engine_seconds,
                                ts=ts, car_id=car_id))
            session.commit()

    @staticmethod
    def get_day_stats(start_ts, end_ts):
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
