#! /usr/bin/env python3

'''--------------------------------------------------------------
Programmer: Nick
Program: 
Date: 
Description: 
--------------------------------------------------------------'''

import os
import json
import boto3
from datetime import datetime, date, timedelta
import sys

def get_paths():

    scripts = os.path.dirname(__file__)
    secrets = "/".join(scripts.split('/')[:-1]) + '/secrets'

    return scripts, secrets

def get_s3_conn(secrets):

    with open(secrets + '/creds.json') as creds:
        creds = json.load(creds)

    s3 = boto3.client(
        's3',
        region_name='us-east-1',
        aws_access_key_id=creds["aws_access_key_id"],
        aws_secret_access_key=creds["aws_secret_access_key"]
    )

    return s3

def get_argv_date_minus_1(argv_1):

    date = datetime.strptime(argv_1, '%Y-%m-%d') - timedelta(days=1)
    date_str = date.strftime('%Y%m%d')
    print('Date = ' + date_str)

    return date_str
