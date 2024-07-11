# Import libraries

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
# Get API Key from Secret Manager
def get_api_keys():
    client = secretmanager.SecretManagerServiceClient()
    keys=[]
    for secret in client.list_secret_versions("secret-path"):

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
# Uploads JSON to GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    destination_blob = dest_prefix+'_'+report_type+'.json'
    
    blob = bucket.blob(destination_blob)
    blob.upload_from_string(output, content_type='application/json')
    print("File uploaded to ", destination_blob)
    return destination_blob

    
def main(event, context):
  
# Depending on the Pub/Sub message, it will perform a different task  
  pubsub_message = base64.b64decode(event['data']).decode('utf-8')
  keys =get_api_keys()
  if str(pubsub_message).split('/')[1] == 'experience_date':
      sdate =(datetime.now() - relativedelta(months=6) -   timedelta(1)).date() # 
  if str(pubsub_message).split('/')[1] == 'booking_date':
      sdate =(datetime.now() - relativedelta(months=6) -   timedelta(1)).date() #
  edate = (datetime.now() - timedelta(1)).date()

# Extract data from the API, loop for every market
  if str(pubsub_message).split('/')[0] == 'Bookings':
    booking_jsons=[]
    s = time.time()
    for k in keys:
        start = time.time()
        print('Processing Booking Summary: ',k.split('--')[1], '- From',  str(sdate), ' to ', str(edate))
        
        if str(pubsub_message).split('/')[1] == 'experience_date':
            print('Processing Booking Summary Based on Experience Date: ',k.split('--')[1], '- From',  str(sdate), ' to ', str(edate))
            booking_summary = requests.get('https://apiURL?from_date='+str(sdate)+'&to_date='+str(edate), 
                                            headers={'X-API-KEY': k.split('--')[0]})
            
        if str(pubsub_message).split('/')[1] == 'booking_date':
            print('Processing Booking Summary Based on Booking Date: ',k.split('--')[1], '- From',  str(sdate), ' to ', str(edate))
            booking_summary = requests.get('https://apiURL?date_type=booking&from_date='+str(sdate)+'&to_date='+str(edate), 
                                    headers={'X-API-KEY': k.split('--')[0]})
# Added time completion report                                
        print('Finished in: ', time.time()-start, 's')
        
        try:
            booking_json=json.loads(booking_summary.text)
            booking_jsons.append([dict(item, **{'account': k.split('--')[1]}) for item in booking_json['data']])
# Added exception for error debuging
        except Exception as ex:
            print('FLAGGGGGGGG-------->', ex)

    all_responses=list(itertools.chain.from_iterable(booking_jsons))
    result = collections.defaultdict(list)
    for i in all_responses:
# Added date to the records
        if str(pubsub_message).split('/')[1] == 'experience_date':
            i.update( {'query_experience_date' :  datetime.strptime(i['attributes']['experience_date'], '%Y-%m-%d').strftime('%Y%m%d') } )
        if str(pubsub_message).split('/')[1] == 'booking_date':
            i.update( {'query_booking_date' :  i['attributes']['booking_date'] } )
        
        i['attributes']['payout_dates']=[x for x in i['attributes']['payout_dates'] if x is not None]
# Define the data that needs to be removed
        remove_PII(( 'guest_id','guest_email', 'guest_first_name', 'guest_last_name', 'guest_phone', 'location'), i['attributes'])
        
        
        if str(pubsub_message).split('/')[1] == 'experience_date':
            result[i['attributes']['experience_date']].append(i)
        if str(pubsub_message).split('/')[1] == 'booking_date':
            result[datetime.strptime(i['attributes']['booking_date'], "%Y-%m-%d  %H:%M:%S").strftime('%Y%m%d')].append(i)
# Concatenate all date in a single file
    for i in sorted(list(result.keys())):
        booking_output = ""
        for n in result[i]:
            booking_output += json.dumps(n) + '\n'
        
        if str(pubsub_message).split('/')[1] == 'experience_date':
            if booking_output != '':
# Load the file to GSC
                bookings_uploaded_file= upload_json_to_gcs(''.join(booking_output), 'some-bucket', i.replace('-',''), 'bookings_expdate')
            else:
                print(day.strftime('%Y%m%d'), ' Not found in any accounts')
                bigquery.Client().delete_table('some-dataset_expdate_'+i.replace('-',''), not_found_ok=True)
        
        if str(pubsub_message).split('/')[1] == 'booking_date':
            if booking_output != '':
                bookings_uploaded_file= upload_json_to_gcs(''.join(booking_output), 'some-bucket', i.replace('-',''), 'bookings_bookdate')
            else:
                print(day.strftime('%Y%m%d'), ' Not found in any accounts')
                bigquery.Client().delete_table('some-dataset_bookdate_'+i.replace('-',''), not_found_ok=True)
    
    print('Finished Pulling Bookings in: ', time.time()-s, 's')
  
# Similar process with Revenue Summary
  if str(pubsub_message).split('/')[0]  == 'Revenue Summary':
    rev_summary_jsons=[]
    s = time.time()
    for k in keys: 
        start = time.time()
        
        if str(pubsub_message).split('/')[1] == 'experience_date':
            print('Processing Revenue Summary Based on Experience Date: ',k.split('--')[1])
            rev_summary = requests.get('https://apiURL?date_type=experience&from_date="2019-01-01T00:00:0000:00"',  
                                headers={'X-API-KEY':k.split('--')[0]})
        if str(pubsub_message).split('/')[1] == 'booking_date': 
            print('Processing Revenue Summary Based on Booking Date: ',k.split('--')[1])
            rev_summary = requests.get('https://apiURL?date_type=booking&from_date="2019-01-01T00:00:0000:00"',  
                                headers={'X-API-KEY':k.split('--')[0]})           
        
        print('Finished in: ', time.time()-start, 's')
        try:
            rev_summary_json=json.loads(rev_summary.text)['data']
            rev_summary_json.update( {'account' : k.split('--')[1]  } )
            rev_summary_jsons.append(rev_summary_json)

        except Exception as ex:
            print('FLAGGGGGGGG-------->', ex)

        
    revenue_output=""
    for i in rev_summary_jsons:
        revenue_output += json.dumps(i) + '\n'
    
    if str(pubsub_message).split('/')[1] == 'experience_date': 
        revenue_uploaded_file= upload_json_to_gcs(revenue_output , 'some-bucket', edate.strftime('%Y%m%d'), 'revenue_expdate')
       
    
    if str(pubsub_message).split('/')[1] == 'booking_date': 
        revenue_uploaded_file= upload_json_to_gcs(revenue_output , 'some-bucket', edate.strftime('%Y%m%d'), 'revenue_bookdate')
    print('Finished Pulling Revenue: ', time.time()-s, 's')
