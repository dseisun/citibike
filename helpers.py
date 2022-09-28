import pandas as pd
from sqlalchemy import create_engine


def get_sample_file(sample_file = 'extracted_data/202207-citbike-tripdata.csv', n=0):
    if n > 0:
        return pd.read_csv(sample_file, header=0, nrows=n)
    else:
        return pd.read_csv(sample_file, header=0)
