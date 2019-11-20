#!/usr/bin/env bash

echo "Running smoke test of all Cloud Functions..."
echo "You should have deployed before this! It won't deploy for you."
echo ""

echo "What's your dev Terra user email address? I'm going to gcloud config set account it:"
read DEV_EMAIL
gcloud config set account "$DEV_EMAIL"
echo ""

echo "Pinging the import service as your dev user..."
JOB_ID=`curl -X POST https://us-central1-broad-dsde-dev.cloudfunctions.net/iservice/general-dev-billing-account/import-service-test/import -H "Authorization: bearer $(gcloud auth print-access-token)" -d '{"path":"buzz", "filetype":"pfb"}'`
echo $JOB_ID
echo "That last line should look like a UUID. If it doesn't, something's broken!"
echo ""

echo "Now I need to be your Broad user to trigger the import tasks through Pub/Sub. What's that email address?"
read BROAD_EMAIL
gcloud config set account "$BROAD_EMAIL"
echo ""

echo "Putting two messages on the PubSub queue as your Broad user..."
gcloud --project broad-dsde-dev pubsub topics publish task_chunk_topic --attribute=job_id="$JOB_ID",boo=fnoo
sleep 2
gcloud --project broad-dsde-dev pubsub topics publish task_chunk_topic --attribute=job_id="$JOB_ID",boo=fnoo
echo "...ok, done."

echo ""

echo "Now go to the following URL. There should be two logs with your UUID at the bottom. It might take a few seconds to show; keep refreshing."
echo "https://console.cloud.google.com/logs/viewer?project=broad-dsde-dev&folder&organizationId&resource=cloud_function%2Ffunction_name%2Ftaskchunk&minLogLevel=0&expandAll=false"
