# Import the required libraries
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.api_core.exceptions import BadRequest

# Define schema for experience date table and experience included table
experience_guests_data_schema = [SchemaField('attributes', 'RECORD', 'NULLABLE', None, (SchemaField('tickets', 'RECORD', 'REPEATED', None)] #Specify all your fields
experience_guests_included_schema = [SchemaField('attributes', 'RECORD', 'NULLABLE', None, (SchemaField('tickets', 'RECORD', 'REPEATED', None)] #Specify all your fields

# Define the function  that loads the Google Storage files into BigQuery
def load_bq(uploaded_file, target_table, schema):
    client = bigquery.Client()
    if schema =='auto':
        job_config = bigquery.LoadJobConfig(autodetect=True)
 
    else:
        job_config = bigquery.LoadJobConfig()
    job_config.schema = schema
# Set write disposition to WRITE_TRUNCATE to overwrite existing data
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

    load_job = client.load_table_from_uri('gs://some-bucket/'+uploaded_file, 'some-project.some-dataset.'+target_table, job_config=job_config) 
    print("Starting job ", {load_job.job_id})
    try:
        load_job.result()  # Waits for table load to complete.
        print("Job finished.")
    except BadRequest as e:
        for e in load_job.errors:
            print('ERROR: {}'.format(e['message']))
# Delete the files from GSC (once they are loaded to BQ)
def delete_gcs_file(uploaded_file):
    """ Uploads JSON to GCS """
    storage_client = storage.Client()
    bucket = storage_client.bucket('some-bucket')
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
# Load the data to BQ
    if str(file['name']).split('/')[0] == 'experience':
        if str(file['name']).split('/')[1] == 'data':
            load_bq(str(file['name']), 'some-dataset_guests_experience_data_'+ str(file['name'])[-13:-5], experience_guests_data_schema) 
        if str(file['name']).split('/')[1] == 'included':
            load_bq(str(file['name']), 'some-dataset_guests_experience_included_'+ str(file['name'])[-13:-5], experience_guests_included_schema) 
 
    delete_gcs_file(str(file['name']))