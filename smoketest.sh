#!/usr/bin/env bash

echo "Running smoke test of all Cloud Functions..."
echo "You should have deployed before this! It won't deploy for you."

echo ""

echo "Pinging the import service..."
JOB_ID=`curl -X POST https://us-central1-broad-dsde-dev.cloudfunctions.net/iservice -H "Authorization: bearer $(gcloud auth print-identity-token)" -d '{"path":"buzz", "filetype":"pfb"}'`
echo $JOB_ID
echo "That last line should look like a UUID. If it doesn't, something's broken!"

echo ""

echo "Putting a message on the PubSub queue..."
PUBLISH_MSG=`gcloud --project broad-dsde-dev pubsub topics publish task_chunk_topic --attribute=job_id="$JOB_ID",boo=fnoo`
echo "...ok, done."

echo ""

echo "Now go to the following URL. You should see your UUID in a log at the bottom. It might take a few seconds to show; keep refreshing."
echo "https://console.cloud.google.com/logs/viewer?project=broad-dsde-dev&folder&organizationId&resource=cloud_function%2Ffunction_name%2Ftaskchunk&minLogLevel=0&expandAll=false"
