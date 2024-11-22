"""Database configuration file"""
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase

# Get DB variables from the environment
DB_USERNAME = os.environ.get('DB_USERNAME')
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_ADDRESS = os.environ.get("DB_ADDRESS")
DB_NAME = os.environ.get("DB_NAME")

# Creating engine and session factory for the whole project
engine = create_engine(f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_ADDRESS}/{DB_NAME}")
Session = sessionmaker(bind=engine)


class Base(DeclarativeBase):
    """Just a standard Base class for models."""
    pass


def db_init():
    """Create all tables if not exit yet."""
    Base.metadata.create_all(engine)


class DataBase:
    def __init__(self):
        self.Session = Session

