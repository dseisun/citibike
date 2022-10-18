# TODO: Need to fix Trips to link to station_name instead of station_id on the stations table
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

def cf_to_trips(cf: CleanedFile) -> DataFrame:
    """Transform a cleaned file to a dataframe ready for loading to the trips table
    using the native pandas to_sql method instead of sqlalchemy (for perf)"""
    return cf.clean_df[[
            'started_at', 
            'ended_at', 
            'start_station_name',
            'end_station_name',
            'membership_status',
            'rideable_type',
            'filename']]

def load_trips_to_db(clean_file: CleanedFile) -> int:
    """Create trips from filename"""
    
    loader = cf_to_trips(clean_file)
    loader.to_sql('trips', engine, schema='prod', if_exists='append', index=False, chunksize=10000)
    return len(clean_file.clean_df)


def cf_to_stations(cf: CleanedFile) -> DataFrame:
    """Given a cleaned file we emit a dataframe ready for loading to the stations table"""
    grouped_df = cf.clean_df.groupby(['start_station_name']) \
        [['start_lat', 'start_lng', 'started_at', 'ended_at', 'start_station_name']] \
        .agg({
            'start_lat': 'mean',
            'start_lng': 'mean',
            'started_at': 'min',
            'ended_at': 'max'}).reset_index()
    grouped_df['filename'] = cf.rawfile.filename
    return grouped_df.rename(columns={
        'start_station_name': 'station_name', 
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

def load_file_to_db(filename: str, load_trips: bool, load_stations: bool, path_prefix='extracted_data'):
    full_path = join(path_prefix, filename)
    """Load files to db"""
    logging.info('Reading file: %s' % filename)
    rf, cf = read_file(full_path)
    if load_trips:
        logging.info('Loading trips from %s' % full_path)
        trips_loaded = load_trips_to_db(cf)
        logging.info('Loaded %s trips to db from %s' % (trips_loaded, full_path))
    if load_stations:
        logging.info('Loading stations from %s' % full_path)
        stations_loaded = load_stations_to_db(cf)
        logging.info('Loaded %s stations to db from %s' % (stations_loaded, full_path))
        logging.info('Merging stations from %s' % full_path)
        merge_stations()
        logging.info('Merged stations from %s' % full_path)

def load_file_header_type(fileheadertype: FileHeaderType, load_trips=True, load_stations=True) -> None:
    for filename in fileheadertype.filerange:
        logging.info('Loading %s file' % filename)
        load_file_to_db(filename, load_trips=load_trips, load_stations=load_stations)

if __name__ == '__main__':
    for fileheadertype in fileheadertypes:
        load_file_header_type(fileheadertype)
    # fh = fileheadertypes[0]
    # file = fh.filerange[0]
    # load_file_to_db(file, load_trips=True, load_stations=True)