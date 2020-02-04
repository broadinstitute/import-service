#! /usr/bin/env bash

echo "The following two operations might fail if the topic and subscription already exist. If this happens, it's fine."

echo ""
echo "Making the pub/sub topic..."

gcloud pubsub topics create import-service-notify

echo ""
echo "Making the pub/sub subscription..."


gcloud pubsub subscriptions create import-service-notify-subscription \
    --topic import-service-notify \
    --push-endpoint \
    "https://import-service-dot-broad-dsde-dev.appspot.com/_ah/push-handlers/receive_messages?token=$(cat token.secret)" \
    --ack-deadline 10 \
    --push-auth-service-account="import-service-pubsub@broad-dsde-dev.iam.gserviceaccount.com" \
    --push-auth-token-audience="importservice.dev.test.firecloud.org"

echo ""
echo ""

echo "Now deploying the app to GAE."
gcloud app deploy --quiet
