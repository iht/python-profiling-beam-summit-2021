# Sample streaming Dataflow pipeline written in Python

This repository contains a streaming Dataflow pipeline written in Python with
Apache Beam, reading data from PubSub.

To run this pipeline, you need to have the SDK installed, and a project in 
Google Cloud Platform, even if you run the pipeline locally with the direct 
runner:
* https://cloud.google.com/sdk/docs/quickstart
* https://cloud.google.com/free

## Description of the pipeline

### Data input

We are using here a public PubSub topic with data, so we don't need to setup
our own to run this pipeline.

The topic is `projects/pubsub-public-data/topics/taxirides-realtime`.

That topic contains messages from the NYC Taxi Ride dataset. Here is a sample
of the data contained in a message in that topic:

```json
{
  "ride_id": "328bec4b-0126-42d4-9381-cb1dbf0e2432",
  "point_idx": 305,
  "latitude": 40.776270000000004,
  "longitude": -73.99111,
  "timestamp": "2020-03-27T21:32:51.48098-04:00",
  "meter_reading": 9.403651,
  "meter_increment": 0.030831642,
  "ride_status": "enroute",
  "passenger_count": 1
}
```

But the messages also contain metadata, that is useful for streaming pipelines.
In this case, the messages contain an attribute of name `ts`, which contains
the same timestamp as the field of name `timestamp` in the data. Remember that
PubSub treats the data as just a string of bytes, so it does not *know*
anything about the data itself. The metadata fields are normally used to publish
messages with specific ids and/or timestamps.

To inspect the messages from this topic, you can create a subscription, and then
pull some messages.

To create a subscription, use the gcloud cli utility (installed by default in
the Cloud Shell):

```
export TOPIC=projects/pubsub-public-data/topics/taxirides-realtime
gcloud pubsub subscriptions create taxis --topic $TOPIC
```

To pull messages:

```gcloud pubsub subscriptions pull taxis --limit 3```

or if you have [jq](https://stedolan.github.io/jq/) (for pretty printing of 
JSON)

```gcloud pubsub subscriptions pull taxis --limit 3 | grep " {" | cut -f 2 -d ' ' | jq```

Pay special attention to the Attributes column (metadata). You will see that
the timestamp included as a field in the metadata, as well as in the
data. We will leverage that metadata field for the timestamps used in
our streaming pipeline.

### Data output

This pipeline writes the output to BigQuery, in streaming append-only mode.

The destination tables must exist prior to running the pipeline.

If you have the GCloud cli utility installed (for instance, it is installed
by default in the Cloud Shell), you can create the tables from the command line.

First create a dataset. Here we locate the dataset of name `scratch` in the EU,
use a location similar to region of Dataflow, to avoid network egress costs.

```bq mk -d --data_location=EU scratch```

The schema of the rides table must match the schema of the PubSub messages
(here we match the schema of the above public PubSub topic)

```
export SCHEMA=ride_id:STRING,point_idx:STRING,latitude:FLOAT,longitude:FLOAT,timestamp:STRING,meter_re
ading:FLOAT,meter_increment:FLOAT,ride_status:STRING,passenger_count:INTEGER
bq mk scratch.rides $SCHEMA
```

Same for the counts table:

```
bq mk scratch.counts ride_id:STRING,count:INTEGER,duration:FLOAT
```

## Algorithm / business rules

We are using a session window with a gap of 10 seconds. That means that all
the messages with the same `ride_id` will be grouped together, as long as
their timestamps are 10 seconds within each other. Any message with a
timestamp more than 10 seconds apart will be discarded (for old timestamps) or
will open a new window (for newer timestamps).

With the messages inside each window (that is, each different `ride_id` will be
part of a different window), we will calculate the duration of the session, as
the difference between the min and max timestamps in the window. We will also
calculate the number of events in that session.

We will use a `GroupByKey` to operate with all the messages in a window. This
will load all the messages in the window into memory. This is fine, as in
Beam streaming, a window is always processed in a worker (windows cannot be
split across different workers).

This is an example of the kind of logic that can be implemented leveraging
windows in streaming pipelines. This grouping of messages across `ride_id` and
event timestamps is automatically done by the pipeline, and we just need to
express the generic operations to be performed with each window, as part of our
pipeline.