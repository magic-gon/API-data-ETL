# Data ETL Pipeline

This repository contains scripts for extracting data from the API, processing it to remove Personally Identifiable Information (PII), and storing it in Google Cloud Storage (GCS) before loading it into BigQuery. The process is automated using Google Cloud Functions and Pub/Sub for triggering and managing data flows.

## Overview

The ETL pipeline consists of five main scripts:

1. **extract_to_bq.py**:
    - Extracts data from Google Cloud Storage and loads it into BigQuery.
    - Utilizes predefined schemas for booking and experience date tables.
    - Manages write operations, schema configurations, and error handling.

2. **extract_api_to_gcs.py**:
    - Extracts data from the API.
    - Processes the data to remove PII.
    - Uploads the processed JSON data to GCS.

3. **guests_data_pipeline.py**:
    - Similar to `extract_api_to_gcs.py`, focused on guest data.
    - Fetches guest data from the API.
    - Removes PII and uploads the processed data to GCS.

4. **pubsub_publisher.py**:
    - Publishes date range messages to a Google Cloud Pub/Sub topic.
    - The messages trigger data extraction processes for different date ranges.

5. **load_experience_guests_data_to_bq.py**:
    - Loads JSON files containing guest experience data from GCS into BigQuery.
    - Defines specific schemas for guest data and included data tables.
    - Manages write operations and error handling.

## Detailed Explanation of Each Script

### extract_to_bq.py

- **Purpose**: Load data from GCS into BigQuery.
- **Functions**:
  - `load_bq(uploaded_file, target_table, schema)`: Loads data into BigQuery.
  - `delete_gcs_file(uploaded_file)`: Deletes files from GCS.
  - `main(event, context)`: Triggered by GCS events to load data into appropriate BigQuery tables.
- **Usage**: Deploy as a Google Cloud Function triggered by GCS object changes.

### extract_api_to_gcs.py

- **Purpose**: Extracts data from the API, removes PII, and uploads it to GCS.
- **Functions**:
  - `get_api_keys()`: Fetches API keys from Secret Manager.
  - `remove_PII(entries, the_dict)`: Removes specified PII fields from data.
  - `upload_json_to_gcs(output, bucket_name, dest_prefix, report_type)`: Uploads JSON data to GCS.
  - `main(event, context)`: Triggered by Pub/Sub messages to extract and process data.
- **Usage**: Deploy as a Google Cloud Function triggered by Pub/Sub messages.

### guests_data_pipeline.py

- **Purpose**: Similar to `extract_api_to_gcs.py`, focuses on guest data.
- **Functions**:
  - `get_api_keys()`: Fetches API keys from Secret Manager.
  - `remove_PII(entries, the_dict)`: Removes specified PII fields from data.
  - `upload_json_to_gcs(output, bucket_name, dest_prefix, report_type)`: Uploads JSON data to GCS.
  - `main(event, context)`: Triggered by Pub/Sub messages to extract and process guest data.
- **Usage**: Deploy as a Google Cloud Function triggered by Pub/Sub messages.

### pubsub_publisher.py

- **Purpose**: Publishes date range messages to a Pub/Sub topic to trigger downstream data extraction processes.
- **Functions**:
  - `send_messages(records_dict)`: Publishes messages to the Pub/Sub topic.
  - `main(event)`: Generates date ranges and publishes them as messages.
- **Usage**: Deploy as a Google Cloud Function triggered manually or by HTTP requests.

### load_experience_guests_data_to_bq.py

- **Purpose**: Load JSON files containing guest experience data from GCS into BigQuery.
- **Functions**:
  - `load_bq(uploaded_file, target_table, schema)`: Loads data into BigQuery.
  - `delete_gcs_file(uploaded_file)`: Deletes files from GCS.
  - `main(event, context)`: Triggered by GCS events to load data into BigQuery.
- **Usage**: Deploy as a Google Cloud Function triggered by GCS object changes.

## Deployment

Each script can be deployed as an individual Google Cloud Function. Ensure you have the necessary Google Cloud resources (Pub/Sub topics, GCS buckets, BigQuery datasets).

Example deployment command:
```sh
# Deploy example for one of the functions
gcloud functions deploy FUNCTION_NAME \
  --runtime python39 \
  --trigger-RESOURCE_TYPE \
  --entry-point FUNCTION_ENTRY_POINT \
  --env-vars-file .env.yaml