from typing import Dict, List
import pandas as pd
import numpy as np
import logging

logging.basicConfig(level=logging.INFO)


class FileHeaderType():
    """Represents a range of files that all have a consistent set of headers"""
    def __init__(self, filerange: List[str], headers: List[str], header_mapping: Dict[str, str]) -> None:
        """Takes a list of file names and an ordered list of headers for those files"""
        self.filerange = filerange
        self.headers = headers
        self.header_mapping = header_mapping

    def contains_file(self, filename: str) -> bool:
        """Returns True if the filename is in the filerange
        We just look for a general string match instead of an exact match
        to simplify matching as it relates to subdirs"""
        for fr in self.filerange:
            if fr in filename:
                return True
        return False

    
    
    def __repr__(self) -> str:
        return '<FileHeader(filename=%s)>' % self.filerange

class RawFile():
    """A processed file is an object that has a filename, a fileheader and does the original loading of the dataframe"""
    def __init__(self, filename, nrows=None) -> None:
        self.filename = filename
        matching_fileheaders: List = [fileheadertype for fileheadertype in fileheadertypes if fileheadertype.contains_file(filename)]
        assert len(matching_fileheaders) != 0, 'No matching fileheaders found for filename: {}'.format(filename)
        assert len(matching_fileheaders) < 2, 'More than one matching fileheaders found for filename: {}'.format(filename)
        self.fileheadertype = matching_fileheaders[0]
        if nrows:
            self.raw_df = pd.read_csv(filename, nrows=nrows)
        else:
            self.raw_df = pd.read_csv(filename)


class CleanedFile():
    """Creates a cleaned dataframe that's ready for final processing to turn into stations and trips"""
    def __init__(self, rawfile: RawFile) -> None:
        self.needed_schema = ['started_at', 'ended_at', 'start_station_name', \
                'start_station_id', 'end_station_name', 'end_station_id', 'start_lat', 'start_lng', 'end_lat', \
                'end_lng', 'membership_status', 'filename']
        self.rawfile = rawfile
        tmp_clean_df = self.set_column_names(rawfile)
        tmp_clean_df['filename'] = rawfile.filename
        tmp_clean_df = self.normalize_station_ids(tmp_clean_df)
        tmp_clean_df = self.set_column_types(tmp_clean_df)
        self.validate_schema(tmp_clean_df) # Validate that we have a mimining set of columns for loading to db
        self.clean_df = tmp_clean_df
        
        
    def set_column_names(self, rf: RawFile):
        """Given a RawFile, map the column names to the cleaned column names defined in the FileHeader"""
        return rf.raw_df.rename(columns=rf.fileheadertype.header_mapping)

    def normalize_station_ids(self, df):
        """Given a dataframe with a column named 'citi_station_id', normalize the values to be strings
        and remove trailing 0's from station_ids
        """
        df['start_station_id'] = df['start_station_id'].astype('string')
        df['end_station_id'] = df['end_station_id'].astype('string')
        df['start_station_id'] = df['start_station_id'].str.rstrip('0')
        df['end_station_id'] = df['end_station_id'].str.rstrip('0')
        return df

    def set_column_types(self, df):
        """Set the column types for the cleaned dataframe"""
        # df['rideable_type'] = df['rideable_type'].astype('string').replace(np.nan, 'None')
        df['started_at'] = pd.to_datetime(df['started_at'])
        df['ended_at'] = pd.to_datetime(df['ended_at'])
        df['start_station_name'] = df['start_station_name'].astype('string').replace(np.nan, 'None')
        # Station_ids are a mix of strings and floats, so we cast all to a string so the groupby doesn't create them as separate records
        df['start_station_id'] = df['start_station_id'].astype('string').replace(np.nan, 'None')
        df['end_station_name'] = df['end_station_name'].astype('string').replace(np.nan, 'None')
        # Station_ids are a mix of strings and floats, so we cast all to a string so the groupby doesn't create them as separate records
        df['end_station_id'] = df['end_station_id'].astype('string').replace(np.nan, 'None')
        df['start_lat'] = df['start_lat'].astype('float').replace(np.nan, 0.0)
        df['start_lng'] = df['start_lng'].astype('float').replace(np.nan, 0.0)
        df['end_lat'] = df['end_lat'].astype('float').replace(np.nan, 0.0)
        df['end_lng'] = df['end_lng'].astype('float').replace(np.nan, 0.0)
        df['membership_status'] = df['membership_status'].astype('string').replace(np.nan, 'None')
        return df

    def validate_schema(self, df) -> None:
        """Validate the schema of the cleaned dataframe"""
        for col in self.needed_schema:
            assert col in df.columns, 'Column {} not found in cleaned dataframe'.format(col)

        if 'ride_id' not in df.columns:
            logging.info('ride_id column not found in file %s. Setting as null' % self.rawfile.filename)
            df['ride_id'] = None

        if 'rideable_type' not in df.columns:
            logging.info('rideable_type column not found in file %s. Setting as null' % self.rawfile.filename)
            df['rideable_type'] = None
    # def load_to_trips(self, engine):
    #     """Load the cleaned dataframe to the trips table"""
    #     self.clean_df.to_sql('trips', engine, schema='prod', if_exists='append', chunksize=1000)
    #     logging.info('Loaded cleaned dataframe to trips table')

    def __repr__(self) -> str:
        return '<CleanedFile(filename=%s)>' % self.rawfile.filename

    

        

fileheadertypes: list[FileHeaderType] = [
    FileHeaderType(
        filerange=["202102-citibike-tripdata.csv", "202103-citibike-tripdata.csv", "202104-citibike-tripdata.csv",
            "202105-citibike-tripdata.csv", "202106-citibike-tripdata.csv", "202107-citibike-tripdata.csv",
            "202108-citibike-tripdata.csv", "202109-citibike-tripdata.csv", "202110-citibike-tripdata.csv",
            "202111-citibike-tripdata.csv", "202112-citibike-tripdata.csv", "202201-citibike-tripdata.csv",
            "202202-citibike-tripdata.csv", "202203-citibike-tripdata.csv", "202204-citibike-tripdata.csv",
            "202205-citibike-tripdata.csv", "202206-citbike-tripdata.csv",  "202207-citbike-tripdata.csv"],
        headers=["ride_id", "rideable_type", "started_at", "ended_at", "start_station_name", "start_station_id",
            "end_station_name", "end_station_id", "start_lat", "start_lng", "end_lat", "end_lng", "member_casual"],
        header_mapping={'member_casual': 'membership_status'}),
    FileHeaderType(
        filerange=['201704-citibike-tripdata.csv','201705-citibike-tripdata.csv','201706-citibike-tripdata.csv',
            '201707-citibike-tripdata.csv','201708-citibike-tripdata.csv','201709-citibike-tripdata.csv',
            '201710-citibike-tripdata.csv','201711-citibike-tripdata.csv','201712-citibike-tripdata.csv',
            '201801-citibike-tripdata.csv','201802-citibike-tripdata.csv','201803-citibike-tripdata.csv',
            '201804-citibike-tripdata.csv','201805-citibike-tripdata.csv','201806-citibike-tripdata.csv',
            '201807-citibike-tripdata.csv','201808-citibike-tripdata.csv','201809-citibike-tripdata.csv',
            '201810-citibike-tripdata.csv','201811-citibike-tripdata.csv','201812-citibike-tripdata.csv',
            '201901-citibike-tripdata.csv','201902-citibike-tripdata.csv','201903-citibike-tripdata.csv',
            '201904-citibike-tripdata.csv','201905-citibike-tripdata.csv','201906-citibike-tripdata.csv',
            '201907-citibike-tripdata.csv','201908-citibike-tripdata.csv','201909-citibike-tripdata.csv',
            '201910-citibike-tripdata.csv','201911-citibike-tripdata.csv','201912-citibike-tripdata.csv',
            '202001-citibike-tripdata.csv','202002-citibike-tripdata.csv','202003-citibike-tripdata.csv',
            '202004-citibike-tripdata.csv','202005-citibike-tripdata.csv','202006-citibike-tripdata.csv',
            '202007-citibike-tripdata.csv','202008-citibike-tripdata.csv','202009-citibike-tripdata.csv',
            '202010-citibike-tripdata.csv','202011-citibike-tripdata.csv','202012-citibike-tripdata.csv',
            '202101-citibike-tripdata.csv'], 
            headers=["tripduration","starttime","stoptime","start station id","start station name","start station latitude","start station longitude","end station id","end station name","end station latitude","end station longitude","bikeid","usertype","birth year","gender"],
            header_mapping={"starttime":"started_at",
                            "stoptime":"ended_at",
                            "start station id":"start_station_id",
                            "start station name":"start_station_name",
                            "start station latitude":"start_lat",
                            "start station longitude":"start_lng",
                            "end station id":"end_station_id",
                            "end station name":"end_station_name",
                            "end station latitude":"end_lat",
                            "end station longitude":"end_lng",
                            "usertype":"membership_status"}
    )
    ]