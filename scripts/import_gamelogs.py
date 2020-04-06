#! /usr/bin/env python3

'''--------------------------------------------------------------
Programmer: Nick
Program: import_gamelogs.py
Description: Read in gamelogs for a single day from MySportsFeeds API
--------------------------------------------------------------'''

import requests
import base64
import json
import sys
from helpers import get_paths, get_s3_conn, get_argv_date_minus_1

def get_scores(date_str, encrypted_api_key_credentials, season="current"):

    # Get URL
    url = 'https://api.mysportsfeeds.com/v2.1/pull/nba/{}/date/{}/player_gamelogs.json'.format(season, date_str)

	# Send request and write json
    try:
        response = requests.get(
            url=url,
            params={
            },
            headers={
                "Authorization": "Basic " + base64.b64encode('{}'.format(encrypted_api_key_credentials).encode('utf-8')).decode('ascii')
            }
        )

        return response
    		
    except requests.exceptions.RequestException:
        print('HTTP Request failed')
   
####################################################################################################

if __name__ == "__main__":

    scripts, secrets = get_paths()

    # Get date from argv
    date_str = get_argv_date_minus_1(sys.argv[1])

    # Get API creds
    with open(secrets + '/creds.json') as json_data:
        creds = json.load(json_data)
    encrypted_api_key_credentials = creds['apikey_token'] + ':' + creds['password']

    # Get raw box scores
    response = get_scores(date_str, encrypted_api_key_credentials)

    # Convert to JSON
    json_file = json.dumps(json.loads(response.text))

    # Write to S3
    s3 = get_s3_conn(secrets)
    print('Writing ' + date_str + '.json')
    s3.put_object(Body=json_file, Bucket='daily-nba-gamelogs', Key='raw_gamelogs/' + date_str + '.json')
