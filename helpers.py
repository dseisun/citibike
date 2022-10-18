import pandas as pd
from sqlalchemy import create_engine


def get_sample_file(sample_file='extracted_data/202207-citbike-tripdata.csv', n=0):
    if n > 0:
        return pd.read_csv(sample_file, header=0, nrows=n)
    else:
        return pd.read_csv(sample_file, header=0)



def paretto(df, col):
    df = df.sort_values(col, ascending=False)
    df['cumsum'] = df[col].cumsum()
    df['cum_perc'] = 100*df['cumsum']/df[col].sum()
    df['tmp'] = 1/len(df)
    df['pct_total'] = df['tmp'].cumsum()
    return df.set_index('pct_total')['cum_perc']



