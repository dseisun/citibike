import pandas as pd
from sqlalchemy import create_engine


def get_sample_file(sample_file = 'extracted_data/202207-citbike-tripdata.csv', top_1000=False):
    if top_1000:
        return pd.read_csv(sample_file, header=0, nrows=1000)
    else:
        return pd.read_csv(sample_file, header=0)
