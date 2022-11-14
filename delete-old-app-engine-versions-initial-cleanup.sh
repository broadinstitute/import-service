#!/bin/bash
#
# Deletes old Google App Engine (GAE) deployments of import-service in an environment. You
# MUST have jq installed to be able to use this script.
#
# USAGE: ./delete-old-app-engine-versions.sh ENV NUMBER_OF_VERSIONS_TO_MAINTAIN
#   ENV must be one of dev, alpha, perf, staging, prod
#   NUMBER_OF_VERSIONS_TO_MAINTAIN how many versions should remain in GAE. GAE will return
#

set -eu
set -o pipefail

# check if colors are supported in the terminal
check_color_support() {
    NCOLORS=$(tput colors)
    if [ "$NCOLORS" -ge 8 ]; then
        BLD="$(tput bold)"
        RED="$(tput setaf 1)"
        GRN="$(tput setaf 2)"
        RST="$(tput sgr0)"
    else
        BLD=""
        RED=""
        GRN=""
        RST=""
    fi
    INFO="${BLD}+${RST}"
}

# print out usage to stdout
usage() {
    printf "Usage: %s ${BLD}ENV${RST}\n  ${BLD}ENV${RST} must be one of dev, alpha, perf, staging, prod.\n" "$0"
    exit 0
}

# print out error with help message to stderr and exit
error() {
    printf "${RED}ERROR: %s${RST}\n\nTry ${BLD}%s --help${RST} to see a list of all options.\n" "$1" "$0" >&2
    exit 1
}

# print out error to stderr and exit
abort() {
    printf "${RED}ABORT: %s${RST}\n" "$1" >&2
    exit 1
}

# ensure that jq is installed
check_jq_installed() {
    if ! jq --version 1>/dev/null 2>&1; then
        abort "jq v1.6 or above is required; install using brew install jq"
    fi
}

# ensure that user has appropriate permissions for app engine
check_user_permissions() {
    FAKE_VERSION="example-invalid-version-for-permission-testing"
    if ! gcloud app versions delete "${FAKE_VERSION}" --project="${NEW_PROJECT}" 1>/dev/null 2>&1; then
        GCLOUD_USER=$(gcloud config get-value account)
        abort "User ${GCLOUD_USER} does not have permissions in ${NEW_PROJECT}"
    fi
}

execute_delete() {
    ALL_VERSIONS=($(gcloud app versions list --project="${NEW_PROJECT}" --format=json | jq -r '.[].version.id'))
    ALL_VERSIONS_COUNT="${#ALL_VERSIONS[@]}"
    VERSIONS_TO_DELETE_COUNT=ALL_VERSIONS_COUNT-REMAINING_VERSIONS
    VERSIONS_TO_DELETE=("${ALL_VERSIONS[@]:0:VERSIONS_TO_DELETE_COUNT}")
    for VARIABLE in ${VERSIONS_TO_DELETE[@]}
    do
      echo "${VARIABLE}"
    done
}

check_color_support

check_jq_installed

if [ -z "${1+:}" ]; then
    usage
fi

case $1 in
    --help ) usage;;
    dev|alpha|staging ) ;;
    prod ) error "This script cannot be run against prod.";;
    * ) error "ENV must be one of dev, alpha, or staging";;
esac

NEW_PROJECT="terra-importservice-$1"
REMAINING_VERSIONS=$2

check_user_permissions

printf "${INFO} Selected project ${GRN}%s${RST}\n" "${NEW_PROJECT}"

execute_delete