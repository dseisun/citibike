from sqlalchemy import create_engine
from sqlalchemy.sql import func
from enum import unique
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, Integer, DateTime, Float, UniqueConstraint

connection_string = 'postgresql://citibike:citibike@/citibike'

engine = create_engine(connection_string, echo=False, future=False) # Works around pandas to_sql bug that breaks with future == "True" - https://github.com/pandas-dev/pandas/issues/40686#issuecomment-872031119
conn = engine.connect()
verbose_engine = create_engine(connection_string, echo=True, future=False)



Base = declarative_base()

class Trip(Base):
    __table_args__ = {'schema': 'prod'}

    __tablename__ = 'trips'

    trip_id = Column(Integer, primary_key=True, autoincrement=True)
    ride_id = Column(String(50))
    started_at = Column(DateTime)
    ended_at = Column(DateTime)
    start_station_id = Column(String(50)) # TODO: Set FK here
    end_station_id = Column(String(50)) # TODO: Set FK here
    membership_status = Column(String(50)) # TODO: Enforce enum here
    rideable_type = Column(String(50)) # TODO: Enforce enum here
    filename = Column(String(50))

    @classmethod
    def create_trip(cls, row):
        return Trip(ride_id=row.ride_id, started_at=row.started_at, ended_at=row.ended_at, \
                start_station_id=row.start_station_id, end_station_id=row.end_station_id, \
                membership_status=row.membership_status, rideable_type=row.rideable_type, \
                    filename=row.filename)
    
    
    def __repr__(self) -> str:
        return '<Trip(trip_id=%s)>' % self.trip_id

class Station(Base):
    __table_args__ = {'schema': 'prod'}
    __tablename__ = 'stations'
    
    station_id = Column(Integer, primary_key=True, autoincrement=True)
    citi_station_id = Column(String(50), unique=True)
    station_name = Column(String(100))
    lat = Column(Float)
    long = Column(Float)
    first_trip_at = Column(DateTime)
    last_trip_at = Column(DateTime)
    created_at = Column(DateTime, server_default=func.now())


class StationStaging(Base):
    __table_args__ = {'schema': 'staging'}
    __tablename__ = 'stations'
    
    citi_station_id = Column(String(50), primary_key=True, unique=True)
    station_name = Column(String(100))
    lat = Column(Float)
    long = Column(Float)
    first_trip_at = Column(DateTime)
    last_trip_at = Column(DateTime)
    filename = Column(String(50))
    created_at = Column(DateTime, server_default=func.now())

class StationLog(Base):
    __table_args__ = {'schema': 'prod'}
    __tablename__ = 'stations_log'
    
    station_log_id = Column(Integer, primary_key=True, autoincrement=True)
    citi_station_id = Column(String(50))
    station_name = Column(String(100))
    lat = Column(Float)
    long = Column(Float)
    first_trip_at = Column(DateTime)
    last_trip_at = Column(DateTime)
    filename = Column(String(50))
    created_at = Column(DateTime, server_default=func.now())


if __name__ == '__main__':
    conn.execute('CREATE SCHEMA IF NOT EXISTS prod')
    conn.execute('CREATE SCHEMA IF NOT EXISTS staging')
    Base.metadata.drop_all(verbose_engine)
    Base.metadata.create_all(verbose_engine)