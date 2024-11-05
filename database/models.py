from sqlalchemy import String, Integer, Boolean, ForeignKey, Float
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from database.database import Base

from typing import List


# class Token(Base):
#     __tablename__ = 'tokens'
#
#     access_token


class Car(Base):
    __tablename__ = 'cars'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(128))
    department: Mapped[str] = mapped_column(String(256), nullable=True)
    model: Mapped[str] = mapped_column(String(50), nullable=True)
    reg_number: Mapped[str] = mapped_column(String(24), nullable=True)
    is_hidden: Mapped[bool] = mapped_column(Boolean, default=False)
    source: Mapped[str] = mapped_column(String(32))
    car_states: Mapped[List['CarState']] = relationship(back_populates='car')


class Sensor(Base):
    __tablename__ = 'sensors'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sensor_name: Mapped[str] = mapped_column(String(50))
    destination: Mapped[int] = mapped_column(Integer)
    sensor_type: Mapped[int] = mapped_column(Integer)


class CarState(Base):
    __tablename__ = 'car_states'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ts: Mapped[int] = mapped_column(Integer)
    is_sent: Mapped[bool] = mapped_column(Boolean)
    lat: Mapped[float] = mapped_column(Float)
    lon: Mapped[float] = mapped_column(Float)
    velocity: Mapped[int] = mapped_column(Integer)
    fuel_level: Mapped[int] = mapped_column(Integer, nullable=True)
    ignition: Mapped[int] = mapped_column(Integer, nullable=True)
    light: Mapped[int] = mapped_column(Integer, nullable=True)
    last_conn: Mapped[int] = mapped_column(Integer)
    car_id: Mapped[int] = mapped_column(ForeignKey('cars.id'))
    car: Mapped['Car'] = relationship(back_populates='car_states')


class Device(Base):
    __tablename__ = 'devices'

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    customer_id: Mapped[str] = mapped_column(String(128))
    name: Mapped[str] = mapped_column(String(128))


class RunTime(Base):
    __tablename__ = 'runtimes'

    id: Mapped[int] = mapped_column(Integer, autoincrement=True, primary_key=True)
    start_ts: Mapped[int] = mapped_column(Integer)
    end_ts: Mapped[int] = mapped_column(Integer)
