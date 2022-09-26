# Write test cases for the following operations:
# - get_stations
# - compare_stations
# - load_to_db

import unittest
import pandas as pd
from pandas import DataFrame
from load_files_to_db import cf_to_stations
from file_header_types import CleanedFile, RawFile, FileHeaderType
from models import verbose_engine, conn
from sqlalchemy.orm import sessionmaker
from models import Trip

sample_filename = 'test_data.csv' 
df: DataFrame = pd.read_csv(sample_filename)
rf = RawFile(sample_filename)
cf = CleanedFile(rf)





class TestDataframeOperations(unittest.TestCase):
    def test_get_stations_returns_right_headers(self):
        """Ensure the cf_to_stations function returns headers that match the staging.stations table columns"""
        self.assertEqual(cf_to_stations(cf).columns.tolist(), [
            'citi_station_id',
            'station_name', 
            'lat', 
            'long', 
            'first_trip_at', 
            'last_trip_at',
            'filename'])    

    def test_get_stations_returns_unique_stations(self):
        stns = cf_to_stations(cf)
        self.assertEqual(len(stns['citi_station_id'].unique()), len(stns['citi_station_id']))


class TestFileLoadingToRawFile(unittest.TestCase):
    def test_file_can_be_loaded_to_rawfile(self):
        self.assertEqual(rf.filename, sample_filename)

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