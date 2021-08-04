#!/usr/bin/env sh

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 DATASET_NAME" >&2
  exit 1
fi

DATASET=$1

bq rm $1.rides
bq mk $1.rides ride_id:STRING,point_idx:STRING,latitude:FLOAT,longitude:FLOAT,timestamp:STRING,meter_reading:FLOAT,meter_increment:FLOAT,ride_status:STRING,passenger_count:INTEGER
bq rm $1.agg
bq mk $1.agg ride_id:STRING,duration:FLOAT,min_timestamp:STRING,max_timestamp:STRING,count:INTEGER,init_status:STRING,end_status:STRING,trigger:STRING,window_start:STRING,window_end:STRING
