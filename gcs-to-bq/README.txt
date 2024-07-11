# BigQuery and Google Storage Integration

This script allows uploading JSON files from Google Cloud Storage into BigQuery tables.

## Required Libraries

- `google.cloud.storage`
- `google.cloud.bigquery`
- `google.api_core.exceptions.BadRequest`

## Schema

The script defines two schemas:
1. `booking_date_schema`
2. `experience_date_schema`

These schemas are used to define specific tables in BigQuery.

## Functions

### `load_bq(uploaded_file, target_table, schema)`

Loads data from a file in Google Cloud Storage into a BigQuery table.

- `uploaded_file`: Name of the uploaded file.
- `target_table`: Target table name in BigQuery.
- `schema`: Schema to use (`'auto'`, `'booking_date_schema'`, `'experience_date_schema'`).

### `delete_gcs_file(uploaded_file)`

Deletes a file from Google Cloud Storage.

- `uploaded_file`: Name of the file to delete.

### `main(event, context)`

Main function triggered by a change in a Cloud Storage bucket. Manages data loading into BigQuery and deletion of processed files.

- `event`: Event payload.
- `context`: Metadata for the event.

## Usage

1. Upload a JSON file to a GCS bucket.
2. The event triggers the `main` function.
3. Depending on the file name, the data is loaded into a specific BigQuery table using the appropriate schema or automatic detection.
4. The processed file is deleted from the GCS bucket.

## Deployment

This script is designed to run as a Google Cloud Function, triggered by Google Cloud Storage events.

```python
# Example deployment with Google Cloud Functions
gcloud functions deploy myFunction \
  --runtime python37 \
  --trigger-resource my-bucket \
  --trigger-event google.storage.object.finalize