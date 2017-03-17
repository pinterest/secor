#!/usr/bin/env bash

BASE_DIR=$(dirname $0)
source ${BASE_DIR}/test_functions.sh


stop_s3

start_s3

initialize
