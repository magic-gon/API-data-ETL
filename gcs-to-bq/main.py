# Import the required libraries
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.api_core.exceptions import BadRequest

# Define schema for Booking date table and experience date table
booking_date_schema = [SchemaField('attributes', 'RECORD', 'NULLABLE', None, (SchemaField('tickets', 'RECORD', 'REPEATED', None)] #Specify all your fields
experience_date_schema = [SchemaField('attributes', 'RECORD', 'NULLABLE', None, (SchemaField('tickets', 'RECORD', 'REPEATED', None)] #Specify all your fields

# Define the function  that loads the Google Storage files into BigQuery
def load_bq(uploaded_file, target_table, schema):
    client = bigquery.Client()
    if schema =='auto':
        job_config = bigquery.LoadJobConfig(autodetect=True)
 
    else:
        job_config = bigquery.LoadJobConfig()
        if schema =='booking_date_schema':
            job_config.schema = booking_date_schema
        if schema =='experience_date_schema':
            job_config.schema = experience_date_schema
# Set write disposition to WRITE_TRUNCATE to overwrite existing data
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

# Load the data from the specified URI into the target table
    load_job = client.load_table_from_uri('gs://some_bucket/'+uploaded_file, 'project-name.dataset-name.'+target_table, job_config=job_config)
    print("Starting job ", {load_job.job_id})
    try:
        load_job.result()  # Waits for table load to complet
        print("Job finished.")
    except BadRequest as e:
        for e in load_job.errors:
            print('ERROR: {}'.format(e['message']))

def delete_gcs_file(uploaded_file):
    """ Uploads JSON to GCS """
    storage_client = storage.Client()
    bucket = storage_client.bucket('some_bucket')
    destination_blob = uploaded_file
    
    blob = bucket.blob(destination_blob)
    blob.delete()
    
def main(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    print(f"Processing file: {file['name']}.")
    
    if str(file['name']).split('.')[0][9:] == 'bookings_bookdate':
    # Load data into the bookings_bookdate table using the booking_date_schema
        load_bq(str(file['name']), 'dataset_bookdate_'+ str(file['name'])[:8], 'booking_date_schema') 


    
    if str(file['name']).split('.')[0][9:] == 'bookings_expdate':
    # Load data into the bookings_expdate table using the experience_date_schema
        load_bq(str(file['name']), 'dataset_expdate_'+ str(file['name'])[:8], 'experience_date_schema') 



    if str(file['name']).split('.')[0][9:] == 'revenue_expdate':
    # Load data into the revenue_expdate table with automatic schema detection
        load_bq(str(file['name']), 'dataset_revenue_expdate', 'auto')
        
    
    if str(file['name']).split('.')[0][9:] == 'revenue_bookdate':
    # Load data into the revenue_bookdate table with automatic schema detection
        load_bq(str(file['name']), 'dataset_revenue_bookdate', 'auto')
    # Delete the processed file from GCS
    delete_gcs_file(str(file['name']))