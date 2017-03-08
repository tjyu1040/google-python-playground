#!/usr/bin/env bash
# Script to install Google Cloud SDK. See https://cloud.google.com/sdk/downloads#linux for details. Also works on Mac OS X.

export CLOUDSDK_CORE_DISABLE_PROMPTS=1
sdk_version=144.0.0

curl https://sdk.cloud.google.com | bash
source $HOME/google-cloud-sdk/path.bash.inc
gcloud components update --version ${sdk_version}
gcloud components install beta
gcloud beta emulators bigtable env-init || true
