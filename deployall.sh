#! /usr/bin/env bash

echo "You must have a file named token.secret in this directory, containing the PUBSUB_TOKEN from app.yaml"

echo "The following three operations might fail if the topic and subscription already exist. If this happens, it's fine."

echo ""
echo "Making the pub/sub topic..."

gcloud pubsub topics create import-service-notify-dev

echo ""
echo "Making the pub/sub subscription..."


gcloud pubsub subscriptions create import-service-notify-dev-subscription \
    --topic import-service-notify-dev \
    --push-endpoint "https://import-service-dot-broad-dsde-dev.appspot.com/_ah/push-handlers/receive_messages?token=$(cat token.secret)" \
    --ack-deadline 600 \
    --push-auth-service-account="import-service@broad-dsde-dev.iam.gserviceaccount.com" \
    --push-auth-token-audience="importservice.dev.test.firecloud.org"


# Pull subscription for FiaBs.
gcloud pubsub subscriptions create import-service-notify-dev-pull \
    --topic import-service-notify-dev \
    --ack-deadline 600


echo ""
echo ""

echo "Now deploying the app to GAE."
gcloud app deploy --quiet
