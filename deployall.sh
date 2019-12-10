#! /usr/bin/env bash


# TODO: make it be okay for these two to fail because they already exist
gcloud pubsub topics create task_chunk_topic
gcloud pubsub subscriptions create task_chunk_subscription \
    --topic task_chunk_topic \
    --push-endpoint \
    "https://import-service-dot-broad-dsde-dev.appspot.com/_ah/push-handlers/receive_messages?token=$(cat token.secret)" \
    --ack-deadline 10

gcloud app deploy
