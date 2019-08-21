#!/usr/bin/env bash

export GOOGLE_APPLICATION_CREDENTIALS=/home/Build_WRF/code/gcs.json

echo "#### Reading running args..."
while getopts ":d:i:m:g:k:v:" option; do
  case "${option}" in
  d) START_DATE=$OPTARG ;;
  k) RUN_ID=$OPTARG ;;
  m) MODE=$OPTARG ;;
  v)
    bucket=$(echo "$OPTARG" | cut -d':' -f1)
    path=$(echo "$OPTARG" | cut -d':' -f2)
    echo "#### mounting $bucket to $path"
    gcsfuse "$bucket" "$path"
    ;;
  esac
done

check_empty() {
  [ -z "$1" ] && echo "" || echo "-$2=$1"
}
echo "START_DATE : $START_DATE"
echo "RUN_ID : $RUN_ID"
echo "MODE : $MODE"
echo "#### Running WRF procedures..."
cd /home/Build_WRF/code
echo "Inside $(pwd)"
python3 wrfv4_run.py \
                    $( check_empty "$START_DATE" start_date ) \
                    $( check_empty "$MODE" mode ) \
                    $( check_empty "$RUN_ID" run_id )
echo "####WRF procedures completed"