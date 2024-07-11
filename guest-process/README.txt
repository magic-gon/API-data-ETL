# Guests Data Pipeline

This script extracts guest data from the API, processes it to remove Personally Identifiable Information (PII), and uploads it to Google Cloud Storage (GCS).

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
- `urllib.parse`

## Environment Setup

- Google Cloud project with Secret Manager configured for storing API keys.
- Google Cloud Storage bucket for storing JSON outputs.

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
- Retrieves required parameters (date type, start date, end date, timestamps).
- Fetches API keys.
- Extracts data from the API.
- Removes PII from the data.
- Uploads processed data to GCS.

## Event Handling

The `main` function handles Pub/Sub messages that trigger data extraction and processing tasks. It takes actions based on the date type (`experience_date` or `booking_date`), start date, and end date provided in the message.

## Example Deployment

This script is designed to run as a Google Cloud Function triggered by Google Cloud Pub/Sub messages.

```sh
# Example deployment with Google Cloud Functions
gcloud functions deploy GuestsDataPipeline \
  --runtime python39 \
  --trigger-topic your-pubsub-topic \
  --entry-point main \
  --env-vars-file .env.yaml