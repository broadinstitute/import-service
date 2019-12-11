#! /usr/bin/env bash

echo "The following two operations might fail if the topic and subscription already exist. If this happens, it's fine."

echo ""
echo "Making the pub/sub topic..."

gcloud pubsub topics create task_chunk_topic

echo ""
echo "Making the pub/sub subscription..."

gcloud pubsub subscriptions create task_chunk_subscription \
    --topic task_chunk_topic \
    --push-endpoint \
    "https://import-service-dot-broad-dsde-dev.appspot.com/_ah/push-handlers/receive_messages?token=$(cat token.secret)" \
    --ack-deadline 10

echo ""
echo ""

echo "Now deploying the app to GAE."
gcloud app deploy --quiet
