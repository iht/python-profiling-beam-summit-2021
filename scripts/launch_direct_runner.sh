#!/usr/bin/env sh

PROFILE_LOCATION=profiling/profile_$(date "+%s")/
PROFILE_REPEAT=100

INPUT_DATA=data/
OUTPUT_LOCATION=output/

python main.py --runner=DirectRunner \
 --profile_cpu \
 --profile_location=$PROFILE_LOCATION \
 --direct_runner_bundle_repeat=$PROFILE_REPEAT \
 --input-data=$INPUT_DATA \
 --output-location=$OUTPUT_LOCATION