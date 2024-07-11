# Experience Guests Data to BigQuery

This script loads JSON files from Google Cloud Storage (GCS) into BigQuery tables. The JSON files contain guest experience data and related included data, processed and uploaded by other parts of the ETL pipeline.

## Required Libraries

- `google.cloud.storage`
- `google.cloud.bigquery`
- `google.api_core.exceptions.BadRequest`

## Schema

The script defines two schemas:
1. `experience_guests_data_schema`
2. `experience_guests_included_schema`

These schemas are used to define specific tables in BigQuery.

## Functions

### `load_bq(uploaded_file, target_table, schema)`

Loads data from a file in Google Cloud Storage into a BigQuery table.

- `uploaded_file`: Name of the uploaded file.
- `target_table`: Target table name in BigQuery.
- `schema`: Schema to use (`experience_guests_data_schema` or `experience_guests_included_schema`).

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
3. Depending on the file name, the data is loaded into a specific BigQuery table using the appropriate schema.
4. The processed file is deleted from the GCS bucket.

## Example Deployment

This script is designed to run as a Google Cloud Function triggered by Google Cloud Storage events.

```sh
# Example deployment with Google Cloud Functions
gcloud functions deploy loadExperienceGuestsDataToBQ \
  --runtime python39 \
  --trigger-resource your-bucket-name \
  --trigger-event google.storage.object.finalize