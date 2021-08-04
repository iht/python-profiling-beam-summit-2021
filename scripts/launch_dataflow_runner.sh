#!/usr/bin/env sh

PROJECT_ID=<YOUR PROJECT_ID>
REGION=us-east1  # Change if you want to run in a different region
TMP_LOCATION=gs://$PROJECT_ID/tmp

PROFILE_LOCATION=gs://$PROJECT_ID/profile_$(date "+%s")/

INPUT_TOPIC=projects/pubsub-public-data/topics/taxirides-realtime
OUTPUT_TABLE_RIDES=taxi_rides.rides
OUTPUT_TABLE_AGG=taxi_rides.agg

python main.py --project=$PROJECT_ID \
 --runner=DataflowRunner \
 --region=$REGION \
 --streaming \
 --use_public_ips \
 --setup_file ./setup.py \
 --temp_location=$TMP_LOCATION \
 --profile_cpu \
 --profile_location=$PROFILE_LOCATION \
 --input-topic=$INPUT_TOPIC \
 --output-table-rides=$OUTPUT_TABLE_RIDES \
 --output-table-agg=$OUTPUT_TABLE_AGG