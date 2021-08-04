#!/usr/bin/env python

#  Copyright 2021 Israel Herraiz
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import apache_beam as beam
import argparse
import json
import sys

from apache_beam.io.gcp import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import window
from apache_beam.transforms import trigger

from dofns import business_rules


def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-topic", required=True)
    parser.add_argument("--output-table-rides")
    parser.add_argument("--output-table-agg")

    app_opts, pipeline_opts = parser.parse_known_args()
    run_pipeline(app_opts, pipeline_opts)


def run_pipeline(app_opts, pipeline_opts):
    input_topic = app_opts.input_topic
    rides_table = app_opts.output_table_rides
    agg_table = app_opts.output_table_agg

    options = PipelineOptions(pipeline_opts)
    with beam.Pipeline(options=options) as p:
        msgs_str = p | "Read from Pubsub" >> beam.io.ReadFromPubSub(
            topic=input_topic,
            timestamp_attribute='ts')
        msgs = msgs_str | "Parse JSON" >> beam.Map(json.loads)
        msgs_withkeys = msgs | "Add keys" >> beam.WithKeys(lambda e: e['ride_id'])
        windowed = msgs_withkeys | "Apply window" >> beam.WindowInto(
            window.Sessions(30),
            trigger=trigger.AfterWatermark(early=trigger.AfterProcessingTime(10)),
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING)
        grouped = windowed | "Grouping by key" >> beam.GroupByKey()
        calculated = grouped | "Apply business rules" >> beam.ParDo(business_rules.BusinessRulesDoFn())

        calculated | "Write agg" >> beam.io.WriteToBigQuery(table=agg_table,
                                                            create_disposition=bigquery.BigQueryDisposition.CREATE_IF_NEEDED,
                                                            method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS)

        msgs | "Write orig" >> beam.io.WriteToBigQuery(table=rides_table,
                                                       create_disposition=bigquery.BigQueryDisposition.CREATE_IF_NEEDED,
                                                       method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS)


if __name__ == '__main__':
    main(sys.argv)
