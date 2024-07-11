# Guests Pub/Sub Publisher

This script generates a series of date ranges and publishes them as messages to a Google Cloud Pub/Sub topic. These messages can then trigger further data processing tasks.

## Required Libraries

- `datetime`
- `warnings`
- `google.cloud.pubsub_v1`
- `json`
- `dateutil.relativedelta`

## Environment Setup

- Google Cloud project with Pub/Sub topic configured.

## Functions

### `send_messages(records_dict)`

Publishes messages to the specified Pub/Sub topic.

- `records_dict`: List of records to publish.

### `main(event)`

Main function that generates date ranges and publishes them as messages. It calculates the start and end dates based on the current date, creates messages containing these dates, and uses `send_messages` to publish them to the Pub/Sub topic.

## Example Deployment

This script is designed to run as a Google Cloud Function triggered manually or by some other events. 

```sh
# Example deployment with Google Cloud Functions
gcloud functions deploy GuestsPubSubPublisher \
  --runtime python39 \
  --trigger-http \
  --entry-point main \
  --env-vars-file .env.yaml