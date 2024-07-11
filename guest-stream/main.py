# Import libraries
from datetime import date, timedelta,datetime 
import warnings
from google.cloud import pubsub_v1
import json
from dateutil.relativedelta import *
warnings.filterwarnings("ignore")
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path('some-project', 'some-pubsub')

def send_messages(records_dict):
# Iterate over each record/message and publish it 
    for record in records_dict:
        data = str(record)

        # Data must be a bytestring
        data = data.encode("utf-8")
        print("Sent: " + str(data))
        # Publish the message
        future = publisher.publish(
            topic_path, data
        )

def main(event): 
  
  start_date =(datetime.now() - relativedelta(days=7) -   timedelta(0)).date() 
  end_date =(datetime.now() - relativedelta(days=0) -   timedelta(0)).date() 
#   end_date = (datetime.now() - timedelta(1)).date() 

  # Prepare experience dates and message then send it 
  updated_dates = [ { 'start_date':(start_date + timedelta(n)).strftime('%Y-%m-%d'),
                 'end_date':(start_date + timedelta(n)).strftime('%Y-%m-%d'),
                  # Empty timestamps for exp as it is ignored per documentation
                 'start_timestamp':'T00:00:00Z',
                 'end_timestamp':'T23:59:59Z',
                 'date_type':'experience'
                } for n in range(int ((end_date - start_date).days))]
  
  send_messages(updated_dates)
  return '{}!'.format('Message sent.') 