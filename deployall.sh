#! /bin/bash

#todo: pass in env vars. see https://cloud.google.com/sdk/gcloud/reference/functions/deploy#--env-vars-file

pushd functions
gcloud --quiet functions deploy iservice --runtime python37 --timeout 540s --trigger-http
popd
