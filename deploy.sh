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
    elif [ "$GIT_BRANCH" == "staging" ]; then
        TARGET_ENV="staging"
    elif [ "$GIT_BRANCH" == "master" ]; then
        TARGET_ENV="prod"
    else
        echo "Git branch '$GIT_BRANCH' is not configured to automatically deploy to a target environment"
        exit 1
    fi
fi

if [[ "$TARGET_ENV" =~ ^(dev|alpha|staging|prod)$ ]]; then
    ENVIRONMENT=${TARGET_ENV}
else
    echo "Unknown environment: $TARGET_ENV - must be one of [dev, alpha, staging, prod]"
    exit 1
fi

echo "Deploying branch '${GIT_BRANCH}' to ${ENVIRONMENT}"

set -x

GOOGLE_PROJECT=terra-importservice-${ENVIRONMENT}

# Get the key for the service account used for deploying
docker run --rm -e VAULT_TOKEN=${VAULT_TOKEN} broadinstitute/dsde-toolbox vault read --format=json "secret/dsde/firecloud/$ENVIRONMENT/import-service/deployer.json" | jq .data > deployer.json

if [ ! -s deployer.json ]; then
    echo "Failed to create deployer.json"
    exit 1
fi


export DSDE_TOOLBOX_DOCKER_IMG=broadinstitute/dsde-toolbox:consul-0.20.0
docker pull $DSDE_TOOLBOX_DOCKER_IMG

# render configs
docker run -v $PWD:/app \
  -e RUN_CONTEXT=live \
  -e INPUT_PATH=/app \
  -e OUT_PATH=/app \
  -e VAULT_TOKEN=${VAULT_TOKEN} \
  -e ENVIRONMENT=${ENVIRONMENT} \
   $DSDE_TOOLBOX_DOCKER_IMG render-templates.sh

# DDO-3178 - due to issues with legacy jenkins nodes effecting the ability to auth to gcr.io registries, 
# switching to the dockerhub mirror of the gcloud sdk image. These are the same images, just hosted on dockerhub.
export CLOUD_SDK_DOCKER_IMG=google/cloud-sdk:403.0.0
docker pull $CLOUD_SDK_DOCKER_IMG


# Deploy the app to the specified project
# If this deploy fails with the following error message:
#    ERROR: (gcloud.app.deploy) Permissions error fetching application [apps/$GOOGLE_PROJECT]. Please make sure you are using the correct project ID and that you have permission to view applications on the project.
# This could be due to the service account described by deployer.json either being destroyed and re-created in the project.

docker run -v $PWD:/app \
  -e GOOGLE_PROJECT=${GOOGLE_PROJECT} \
  -w /app \
  --entrypoint "/bin/bash" \
   $CLOUD_SDK_DOCKER_IMG \
  -c "ls app && gcloud auth activate-service-account --key-file=deployer.json --project=$GOOGLE_PROJECT && gcloud app deploy app.yaml cron.yaml --project=$GOOGLE_PROJECT --no-cache"
