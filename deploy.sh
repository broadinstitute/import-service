#!/usr/bin/env bash
set -e
set -x

VAULT_TOKEN=$1
GIT_BRANCH=$2
TARGET_ENV=$3

set +x
if [ -z "$TARGET_ENV" ]; then
    echo "TARGET_ENV argument not supplied; inferring from GIT_BRANCH '$GIT_BRANCH'."

    if [ "$GIT_BRANCH" == "develop" ]; then
        TARGET_ENV="dev"
    elif [ "$GIT_BRANCH" == "alpha" ]; then
        TARGET_ENV="alpha"
    elif [ "$GIT_BRANCH" == "perf" ]; then
        TARGET_ENV="perf"
    elif [ "$GIT_BRANCH" == "staging" ]; then
        TARGET_ENV="staging"
    elif [ "$GIT_BRANCH" == "master" ]; then
        TARGET_ENV="prod"
    else
        echo "Git branch '$GIT_BRANCH' is not configured to automatically deploy to a target environment"
        exit 1
    fi
fi

if [[ "$TARGET_ENV" =~ ^(dev|alpha|perf|staging|prod)$ ]]; then
    ENVIRONMENT=${TARGET_ENV}
else
    echo "Unknown environment: $TARGET_ENV - must be one of [dev, alpha, perf, staging, prod]"
    exit 1
fi

echo "Deploying branch '${GIT_BRANCH}' to ${ENVIRONMENT}"
set -x

GOOGLE_PROJECT=terra-importservice-${ENVIRONMENT}
IMPORT_SERVICE_IMAGE=quay.io/broadinstitute/import-service:${GIT_BRANCH}

docker run --rm -e VAULT_TOKEN=${VAULT_TOKEN} broadinstitute/dsde-toolbox vault read --format=json "secret/dsde/firecloud/$ENVIRONMENT/import-service/import-service-account.json" | jq .data > import-service-account.json

if [ ! -s import-service-account.json ]; then
    echo "Failed to create import-service-account.json"
    exit 1
fi

docker pull ${IMPORT_SERVICE_IMAGE}

export DSDE_TOOLBOX_DOCKER_IMG=broadinstitute/dsde-toolbox:consul-0.20.0
docker pull $DSDE_TOOLBOX_DOCKER_IMG

docker run -v $PWD:/app \
  -e RUN_CONTEXT=live \
  -e INPUT_PATH=/app \
  -e OUT_PATH=/app \
  -e VAULT_TOKEN=${VAULT_TOKEN} \
  -e ENVIRONMENT=${ENVIRONMENT} \
  -e ENV=dev \
   $DSDE_TOOLBOX_DOCKER_IMG render-templates.sh

# deploy the app to the specified project
docker run -v $PWD/app.yaml:/app/app.yaml \
  -v $PWD/import-service-account.json:/app/import-service-account.json \
  -e GOOGLE_PROJECT=${GOOGLE_PROJECT} \
  --entrypoint "/bin/bash" \
  ${IMPORT_SERVICE_IMAGE} \
  -c "gcloud auth activate-service-account --key-file=import-service-account.json && gcloud -q app deploy app.yaml --project=$GOOGLE_PROJECT && gcloud -q app deploy cron.yaml --project=$GOOGLE_PROJECT"
