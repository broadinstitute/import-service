#! /usr/bin/env bash

gcloud --quiet functions deploy iservice --runtime python37 --timeout 540s --trigger-http --env-vars-file="secrets.yaml"
gcloud --quiet functions deploy taskchunk --runtime python37 --timeout 540s --trigger-topic task_chunk_topic --env-vars-file="secrets.yaml"
