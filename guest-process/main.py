# Import the libraries
import requests
from datetime import date, timedelta,datetime
import json
import os
from google.cloud import storage
from google.cloud import bigquery
from google.cloud import secretmanager
from google.api_core.exceptions import BadRequest
from google.cloud.bigquery import SchemaField
import time
import itertools
import collections
import base64
from dateutil.relativedelta import *
from urllib.parse import urlparse
from urllib.parse import parse_qs
# Get API Key from Secret Manager
def get_api_keys():
    client = secretmanager.SecretManagerServiceClient()
    keys=[]
    for secret in client.list_secret_versions("path_to_secret"):

        if secret.state == 1:
            response = client.access_secret_version(str(secret.name))
            key = response.payload.data.decode('UTF-8')

            keys.append(key)
    return keys
# Define function to delete PII
def remove_PII(entries, the_dict):
    for key in entries:
        if key in the_dict:
            del the_dict[key]
# Define function to upload extracted data from API to GCS
def upload_json_to_gcs(output, bucket_name, dest_prefix,report_type):
    """ Uploads JSON to GCS """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    destination_blob = dest_prefix+report_type+'.json'
    
    blob = bucket.blob(destination_blob)
    blob.upload_from_string(output, content_type='application/json')
    print("File uploaded to ", destination_blob)
    return destination_blob


def main(event, context):
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_message)
    pubsub_message_cleaned= eval(pubsub_message)
# Get date type (experience or booking) and date and timestamp
    date_type = pubsub_message_cleaned['date_type']
    start_date = pubsub_message_cleaned['start_date']
    end_date = pubsub_message_cleaned['end_date']
    start_timestamp = pubsub_message_cleaned['start_timestamp']
    end_timestamp = pubsub_message_cleaned['end_timestamp']
    keys = get_api_keys()

    guests_data_responses=[]
    guests_included_responses=[]
# Loop through accounts and keys
    for k in keys:
        print(k.split('--')[1])
        print(date_type,start_date,start_timestamp, end_date, end_timestamp)
        url = 'https://some-api?date_type={}&from_date={}{}&to_date={}{}&include=marketing_optins,checkout_answers,previsit_purchase_behavior_answers,postvisit_feedback_answers&page{}'.format('updated',start_date,start_timestamp, end_date,end_timestamp,'1')
        print(url)
        guests = requests.get(  url, headers={'X-API-KEY': k.split('--')[0]})
        print(guests.status_code)
        if guests.status_code == 200:
            print(guests.json().keys())
            guests_data_response= guests.json()['data']
            guests_included_response= guests.json()['included']
            guests_data_responses.append([dict(item, **{'account': k.split('--')[1]}) for item in guests_data_response])
            guests_included_responses.append([dict(item, **{'account': k.split('--')[1]}) for item in guests_included_response])
# For loop to grab guests data in pages
            while guests.json()['links']["next"]: 
                next_url =guests.json()['links']["next"]
                print(next_url)
                next_url_formatted = next_url.split('?')[0] +'?include=checkout_answers,previsit_purchase_behavior_answers,postvisit_feedback_answers&' +next_url.split('?')[1]
                print(next_url_formatted)
                guests = requests.get(next_url_formatted, headers={'X-API-KEY': k.split('--')[0]})
                print(guests.json().keys())
                guests_data_response= guests.json()['data']
                guests_included_response= guests.json()['included']
                guests_data_responses.append([dict(item, **{'account': k.split('--')[1]}) for item in guests_data_response])
                guests_included_responses.append([dict(item, **{'account': k.split('--')[1]}) for item in guests_included_response])
               
                    
            
# Remove PII fields
    all_guest_data_responses=list(itertools.chain.from_iterable(guests_data_responses))
    all_guest_included_responses=list(itertools.chain.from_iterable(guests_included_responses))
    for i in all_guest_data_responses:
        remove_PII(( 'first_name','last_name', 'email', 'phone', 'street_address', 'city', 'state_province', 'postal_code','birthdate', 'gender'), i['attributes'])

    guests_data_output = ""
    for i in all_guest_data_responses:
        guests_data_output += json.dumps(i) + '\n'

    guests_included_output = ""
    for i in all_guest_included_responses:
        guests_included_output += json.dumps(i) + '\n'
# Upload data to GCS
    uploaded_data_file= upload_json_to_gcs(guests_data_output , 'some-bucket', '{}/{}/'.format(date_type, 'data'), 'guests_data_{}'.format(end_date.replace('-',''))) 
    uploaded_included_file= upload_json_to_gcs(guests_included_output , 'some-bucket', '{}/{}/'.format(date_type,'included'), 'guests_included_{}'.format(end_date.replace('-',''))) 