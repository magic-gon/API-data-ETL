# Data Pipeline

This script extracts data from the API, processes it, and uploads it to Google Cloud Storage (GCS). The data can then be further imported into BigQuery.

## Required Libraries

- `requests`
- `datetime`
- `json`
- `os`
- `google.cloud.storage`
- `google.cloud.bigquery`
- `google.cloud.secretmanager`
- `google.api_core.exceptions.BadRequest`
- `time`
- `itertools`
- `collections`
- `base64`
- `dateutil.relativedelta`

## Environment Setup

- Google Cloud project with Secret Manager configured for storing API keys.
- Google Cloud Storage bucket for storing JSON outputs.
- BigQuery dataset for reporting.

## Functions

### `get_api_keys()`

Fetches the API keys from Google Cloud Secret Manager.

### `remove_PII(entries, the_dict)`

Removes Personally Identifiable Information (PII) from the data.

- `entries`: List of PII fields to remove.
- `the_dict`: Dictionary from which PII needs to be removed.

### `upload_json_to_gcs(output, bucket_name, dest_prefix, report_type)`

Uploads JSON data to Google Cloud Storage.

- `output`: JSON data to upload.
- `bucket_name`: Name of the GCS bucket.
- `dest_prefix`: Destination prefix for the file.
- `report_type`: Type of report being processed.

### `main(event, context)`

Main function triggered by a Pub/Sub message.

- Decodes Pub/Sub message.
- Fetches API keys.
- Depending on message content, sets the date range for data extraction.
- Extracts data from the API based on message content.
- Processes the data, removes PII, and uploads it to GCS.
- Removes the uploaded file from GCS if necessary.

## Event Handling

The `main` function handles Pub/Sub messages to trigger different tasks:

- `Bookings/experience_date`
- `Bookings/booking_date`
- `Revenue Summary/experience_date`
- `Revenue Summary/booking_date`

## Example Deployment

This script is designed to run as a Google Cloud Function triggered by Google Cloud Pub/Sub messages.

```python
# Example deployment with Google Cloud Functions
gcloud functions deploy DataPipeline \
  --runtime python39 \
  --trigger-topic your-pubsub-topic \
  --entry-point main \
  --env-vars-file .env.yaml