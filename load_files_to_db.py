# TODO: Use argparse to give a command line for what to load (single file, directory, etc...)
# TODO: You only populate the stations table from starts, not ends.  If trips only end at a station, it won't be in the stations table
# This tends to happen for maintenance stations
from typing import Any, Dict
from os.path import join
import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from os import listdir
from os.path import join
import logging
from file_header_types import CleanedFile, FileHeaderType, RawFile
from models import StationStaging, engine, conn
from sqlalchemy.orm import Session
from models import Trip
from file_header_types import fileheadertypes
logging.basicConfig(level=logging.INFO)



def read_file(filename):
    rf = RawFile(filename)
    cf: CleanedFile = CleanedFile(rf)
    return (rf, cf)

def cf_to_trips(cf) -> DataFrame:
    """Transform a cleaned file to a dataframe ready for loading to the trips table
    using the native pandas to_sql method instead of sqlalchemy (for perf)"""
    return cf.clean_df[[
            'ride_id',
            'started_at', 
            'ended_at', 
            'start_station_id',
            'end_station_id',
            'membership_status',
            'rideable_type',
            'filename']]

def load_trips_to_db(clean_df: CleanedFile) -> int:
    """Create trips from filename"""
    
    loader = cf_to_trips(clean_df)
    return loader.to_sql('trips', engine, schema='prod', if_exists='append', index=False, chunksize=10000)


def cf_to_stations(cf: CleanedFile) -> DataFrame:
    """Given a cleaned file we emit a dataframe ready for loading to the stations table"""
    grouped_df = cf.clean_df.groupby(['start_station_id']) \
        [['start_lat', 'start_lng', 'started_at', 'ended_at', 'start_station_name']] \
        .agg({
            'start_station_name': 'min', 
            'start_lat': 'mean',
            'start_lng': 'mean',
            'started_at': 'min',
            'ended_at': 'max'}).reset_index()
    grouped_df['filename'] = cf.rawfile.filename
    return grouped_df.rename(columns={
        'start_station_name': 'station_name', 
        'start_station_id': 'citi_station_id',
        'start_lat': 'lat',
        'start_lng': 'long',
        'started_at': 'first_trip_at',
        'ended_at': 'last_trip_at'})

def load_stations_to_db(cf: CleanedFile):
    """Load stations from cleaned file"""
    # Defensively truncating the staging table in case we have a partial load
    with engine.connect() as conn:
        conn.execute(text('truncate table staging.stations'))
    cf_to_stations(cf).to_sql('stations', engine, schema='staging', if_exists='append', index=False, chunksize=10000)

def merge_stations():
    """Merge stations from staging with pre-existing stations on prod"""
    with engine.connect() as conn:
        with open('merge_stations.sql') as f:
            conn.execute(text(f.read()))

def get_files(prefix='extracted_data'):
    """Return a tuple of the absolute path of the file and the filename"""
    return [(join(prefix, filename), filename) for filename in listdir(prefix)]

def load_file_to_db(filename: str, load_trips, load_stations):
    """Load files to db"""
    logging.info('Reading file: %s' % filename)
    rf, cf = read_file(filename)
    if load_trips:
        logging.info('Loading trips from %s' % filename)
        trips_loaded = load_trips_to_db(cf)
        logging.info('Loaded %s trips to db from %s' % (trips_loaded, filename))
    if load_stations:
        logging.info('Loading stations from %s' % filename)
        stations_loaded = load_stations_to_db(cf)
        logging.info('Loaded %s stations to db from %s' % (stations_loaded, filename))
        logging.info('Merging stations from %s' % filename)
        merge_stations()
        logging.info('Merged stations from %s' % filename)

def load_file_header_type(fileheadertype: FileHeaderType, path_prefix='extracted_data', load_trips=True, load_stations=True) -> None:
    for filename in fileheadertype.filerange:
        logging.info('Loading %s file' % filename)
        load_file_to_db(join(path_prefix, filename), load_trips=True, load_stations=True)

if __name__ == '__main__':
    for fileheadertype in fileheadertypes:
        load_file_header_type(fileheadertype)