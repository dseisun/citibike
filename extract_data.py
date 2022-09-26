# What do you want to do
# Get the index of citibike files, pull them, extract them
# Helpful link https://medium.com/@fausto.manon/building-a-citibike-database-with-python-9849a59fb90c
import pandas as pd
import requests
from bs4 import BeautifulSoup
import csv
import zipfile
import logging



index_url = 'https://s3.amazonaws.com/tripdata/'

def get_keys(index_url):
    """Takes an index xml file, parses it and gets a list of all the files to download"""
    resp = requests.get(index_url)
    parsed_xml = BeautifulSoup(resp.text, 'xml')
    return [content.Key.text for content in parsed_xml.ListBucketResult.find_all('Contents')]

def pull_all_files(index_url, path='data/'):
    """Hits the citibike index and downloads all the files to the path provided
    Returns a list of filenames downloaded"""
    keys = get_keys(index_url)
    for key in keys:
        with open('data/%s' % key, 'wb') as outfile:
            resp = requests.get(url + key)
            outfile.write(resp.content)
    return keys

for key in keys:
    with zipfile.ZipFile('data/%s' % key, 'r') as ref:
        files = list(filter(lambda x: x.endswith('.csv') and not x.startswith('__'), ref.namelist()))
        if len(files) == 1:
            file = files[0]
            ref.extract(file, path='extracted_data/')
        else:
            logging.ERROR('Multiple eligible files found in %s. Not extracting' % key)