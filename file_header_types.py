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