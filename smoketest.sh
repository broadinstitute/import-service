#!/usr/bin/env bash

echo "Running smoke test of all Cloud Functions..."
echo "You should have deployed before this! It won't deploy for you."

echo ""

echo "Pinging the import service..."
PING_ISERVICE=`curl -X POST https://us-central1-broad-dsde-dev.cloudfunctions.net/iservice -H "Authorization: bearer $(gcloud auth print-identity-token)" -d '{"path":"buzz", "filetype":"pfb"}'`
echo $PING_ISERVICE
echo "That last line should look like a UUID. If it doesn't, something's broken!"

echo ""

echo "Putting a message on the PubSub queue..."

# [to be continued...]