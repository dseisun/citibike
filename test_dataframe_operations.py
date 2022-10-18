# Write test cases for the following operations:
# - get_stations
# - compare_stations
# - load_to_db

import unittest
import pandas as pd
from pandas import DataFrame
import pandas as pd
from load_files_to_db import cf_to_stations
from file_header_types import CleanedFile, RawFile, FileHeaderType, fileheadertypes
from models import verbose_engine, conn
from sqlalchemy.orm import sessionmaker
from models import Trip
import helpers
from collections import namedtuple
from os.path import join

FileheaderTestObjects = namedtuple('FileheaderTestObjects', ['filename', 'raw_file', 'clean_file'])
prefix = 'extracted_data'

fileheaders_test_data: list[FileheaderTestObjects]  = [] 
for fileheadertype in fileheadertypes:
    filename = join(prefix, fileheadertype.filerange[0])
    raw_file = RawFile(filename)
    cleaned_file = CleanedFile(raw_file)
    fileheaders_test_data.append(FileheaderTestObjects(filename, raw_file, cleaned_file))

    
sample_filename_2022 = 'extracted_data/202207-citbike-tripdata.csv'
sample_filename_2020 = 'extracted_data/202101-citibike-tripdata.csv'
df: DataFrame = pd.read_csv(sample_filename_2022, nrows=1000)
rf = RawFile(sample_filename_2022)
cf = CleanedFile(rf)


class TestDataframeOperations(unittest.TestCase):
    
    def test_get_stations_returns_right_headers(self):
        """Ensure the cf_to_stations function returns headers that match the staging.stations table columns"""
        for fileheader_test_data in fileheaders_test_data:
            self.assertEqual(cf_to_stations(fileheader_test_data.clean_file).columns.tolist(), [
                'station_name', 
                'lat', 
                'long', 
                'first_trip_at', 
                'last_trip_at',
                'filename'])    

    def test_get_stations_returns_unique_stations(self):
        stns = cf_to_stations(cf)
        self.assertEqual(len(stns['station_name'].unique()), len(stns['station_name']))


class TestFileLoadingToRawFile(unittest.TestCase):
    def test_file_can_be_loaded_to_rawfile(self):
        self.assertEqual(rf.filename, sample_filename_2022)

    def test_file_matches_fileheadertype(self):
        self.assertIsInstance(rf.fileheadertype, FileHeaderType)

class TestFileProcessingToCleanFile(unittest.TestCase):
    
    def test_file_can_be_processed_to_cleanedfile(self):
        self.assertIsInstance(cf, CleanedFile)

    def test_file_has_correct_column_names(self):
        self.assertEqual(cf.clean_df.columns.tolist(), 
        ['ride_id', 
        'rideable_type', 
        'started_at', 
        'ended_at', 
        'start_station_name', 
        'start_station_id', 
        'end_station_name', 
        'end_station_id', 
        'start_lat', 
        'start_lng', 
        'end_lat', 
        'end_lng', 
        'membership_status', 
        'filename'])

class TestLoadingTrips(unittest.TestCase):
    
    def test_loading_trips_to_db(self):
        pass
        

if __name__ == '__main__':
    unittest.main()