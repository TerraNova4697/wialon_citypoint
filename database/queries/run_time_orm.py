import logging
import os

from sqlalchemy.exc import MultipleResultsFound, NoResultFound

from database.database import Session
from database.models import RunTime


logger = logging.getLogger(os.environ.get("LOGGER"))


class RunTimeORM:

    @staticmethod
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

    @staticmethod
    def create_runtime(start_ts, end_ts):
        with Session() as session:
            session.add(RunTime(start_ts=start_ts, end_ts=end_ts))
            session.commit()
