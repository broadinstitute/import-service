#! /bin/bash

pushd functions
gcloud --quiet functions deploy iservice --runtime python37 --timeout 540s --trigger-http
popd