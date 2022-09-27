# TODO: Add fix to create the data and extracted data path if they don't exist
# What do you want to do
# Get the index of citibike files, pull them, extract them
# Helpful link https://medium.com/@fausto.manon/building-a-citibike-database-with-python-9849a59fb90c
from typing import Dict, List
import pandas as pd
import requests
from bs4 import BeautifulSoup
import csv
import zipfile
import logging
import os
import bs4


index_url = 'https://s3.amazonaws.com/tripdata/'
data_path = 'data/'




def get_keys_from_index(index_url) -> List[str]:
    """Takes an index xml file, parses it and gets a list of all the files to download"""
    logging.info('Getting keys from index')
    resp = requests.get(index_url)
    parsed_xml = BeautifulSoup(resp.text, 'xml')
    return [content.Key.text for content in parsed_xml.ListBucketResult.find_all('Contents')]

def pull_all_files(index_url) -> List[str]:
    """Hits the citibike index and downloads all the files to the path provided
    Returns a list of filenames downloaded"""
    keys: list[str] = get_keys_from_index(index_url)
    keys_to_files: Dict[str, str] = {}
    for key in keys:
        download_path = os.path.join(data_path, key)
        keys_to_files[key] = download_path
        with open(download_path, 'wb') as outfile:
            resp = requests.get(index_url + key)
            outfile.write(resp.content)
    return keys

def extract_all_files():
    """Extracts all the zip files in the path provided"""
    for file in os.listdir(data_path):
        if file.endswith('.zip'):
            with zipfile.ZipFile(os.path.join(data_path, file), 'r') as zip_ref:
                # Return a list of files in the zip that are candidates for extraction. We expect only 1
                files = list(filter(lambda x: x.endswith('.csv') and not x.startswith('__'), zip_ref.namelist()))
                if len(files) == 1:
                    file = files[0]
                    zip_ref.extract(file, path='extracted_data/')
                else:
                    logging.error('Multiple eligible files found in %s. Not extracting' % file)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    pull_all_files(index_url)
    extract_all_files()