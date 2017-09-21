#!/bin/bash

## List of dates to fetch
declare -a dates=("2017-01-26" "2017-01-27" "2017-01-28" "2017-01-29" "2017-01-30" "2017-01-31")

for day in "${dates[@]}"
do
    echo "starting day ${day}"
    ## Create folder
    mkdir -p /tmp/s3bu/${day}
    ## download from s3
    s3cmd get --recursive s3://giosg-secor-prod/backup/customer_events_prod/dt=${day}/ /tmp/s3bu/${day}/
done