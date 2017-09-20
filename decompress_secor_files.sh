#!/bin/bash
DATE=2017-01-07
# Make sure that out folder exists
SECOR_OUT=/tmp/s3bu/${DATE}/fixed/
mkdir -p $SECOR_OUT

# Go over all of the gz files in the folder with 
# secor file printer tool
FILES=/tmp/s3bu/${DATE}/*.gz
for f in $FILES
do
  echo "Processing $f file..."
  # Extract only the filename from the file
  current_filename=$(basename ${f} .gz) 
  output_filename="${SECOR_OUT}${current_filename}_decompressed.txt"
  # Use tool to read file and extract to output/original_filename_decompressed.txt
  java -ea -Dlog4j.configuration=log4j.prod.properties -Dconfig=secor.prod.backup.properties -cp "secor-0.25-SNAPSHOT.jar:lib/*" com.pinterest.secor.main.LogFilePrinterMain -f $f > ${output_filename}
done
