#! /usr/bin/env python3

'''--------------------------------------------------------------
Programmer: Nick
Program: parse_gamelogs.py
Description: Read in JSON data and load parquet gamelogs
--------------------------------------------------------------'''

import json
import sys
import pandas as pd
from pandas.io.json import json_normalize
from helpers import get_paths, get_s3_conn, get_argv_date_minus_1

def extract_gamelogs(s3, bucket, date_str):

    # Read in JSON
    s3_clientobj = s3.get_object(Bucket=bucket, Key='raw_gamelogs/' + date_str + '.json')['Body'].read().decode('utf-8')
    s3_clientobjdata = json.loads(s3_clientobj)
    gamelogs = s3_clientobjdata['gamelogs']

     # Convert to pandas DataFrame
    df = json_normalize(gamelogs)

    # Replace spaces in column names with underscores
    df.columns = df.columns.str.replace(".", "_")

    # Add a date column
    df['file_date'] = date_str

    return df

def write_parquet_to_csv(df, bucket, folder, date_str):

    df.to_csv('s3://' + bucket + '/' + folder + '/' + date_str + '.csv')


####################################################################################################

if __name__ == "__main__":

    bucket = 'daily-nba-gamelogs'
    folder = 'raw_parsed_gamelogs'
    scripts, secrets = get_paths()
    s3 = get_s3_conn(secrets)

    # Get date from argv
    date_str = get_argv_date_minus_1(sys.argv[1])

    # Read in data
    df = extract_gamelogs(s3, bucket, date_str)

    # Write to CSV
    write_parquet_to_csv(df, bucket, folder, date_str)
