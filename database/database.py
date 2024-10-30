import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase

DB_USERNAME = os.environ.get('DB_USERNAME')
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_ADDRESS = os.environ.get("DB_ADDRESS")
DB_NAME = os.environ.get("DB_NAME")

engine = create_engine(f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_ADDRESS}/{DB_NAME}")

Session = sessionmaker(bind=engine)


class Base(DeclarativeBase):
    pass


def db_init():
    Base.metadata.create_all(engine)


class DataBase:
    def __init__(self):
        self.Session = Session

