#!/usr/bin/env bash

if [[ $# -ne 3 ]]; then
    echo "Usage: ./smoketest.sh DEV_TERRA_EMAIL BROAD_EMAIL namespace/workspace"
    echo "DEV_TERRA_EMAIL: your test email@gmail.com registered in terra-dev"
    echo "BROAD_EMAIL: your personal @broadinstitute.org email, which should be an editor on broad-dsde-dev"
    echo "namespace/workspace: A workspace you have write access to in Terra dev"
    echo ""
    echo "Note that you need to have gcloud auth login'd with both DEV_TERRA_EMAIL and BROAD_EMAIL -- they should show up in gcloud auth list."
    exit 2
fi

DEV_EMAIL=$1
BROAD_EMAIL=$2

IFS='/' read -ra WS_ARR <<< "$3"

WS_NAMESPACE=${WS_ARR[0]}
WS_NAME=${WS_ARR[1]}

echo "Running smoke test of all Cloud Functions..."
echo "You should have deployed before this! It won't deploy for you."
echo ""

echo "Pinging the import service as your dev user..."
gcloud config set account $DEV_EMAIL
JOB_ID=`curl -X POST "https://import-service-dot-broad-dsde-dev.appspot.com/iservice/$WS_NAMESPACE/$WS_NAME/import" -H "Authorization: bearer $(gcloud auth print-access-token)" -d '{"path":"buzz", "filetype":"pfb"}'`
echo $JOB_ID
echo "That last line should look like a UUID. If it doesn't, something's broken!"
echo ""

echo "Putting two messages on the PubSub queue as your Broad user..."
gcloud config set account $BROAD_EMAIL
gcloud --project broad-dsde-dev pubsub topics publish task_chunk_topic --attribute=job_id="$JOB_ID",boo=fnoo
sleep 2
gcloud --project broad-dsde-dev pubsub topics publish task_chunk_topic --attribute=job_id="$JOB_ID",boo=fnoo
echo "...ok, done."

echo ""

echo "Now go to the following URL. There should be two logs with your UUID at the bottom. It might take a few seconds to show; keep refreshing."
echo "https://console.cloud.google.com/logs/viewer?project=broad-dsde-dev&folder&organizationId&resource=cloud_function%2Ffunction_name%2Ftaskchunk&minLogLevel=0&expandAll=false"
