"""
Smallest possible Beam pipeline: read from Pub/Sub, print raw messages.
Proves the connection works before adding windowing or BigQuery writes.
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

PROJECT_ID = "harsha-data-platform"
SUBSCRIPTION = f"projects/{PROJECT_ID}/subscriptions/peakcart-order-events-sub"


def run():
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION)
            | "DecodeBytes" >> beam.Map(lambda raw_bytes: raw_bytes.decode("utf-8"))
            | "PrintMessage" >> beam.Map(print)
        )


if __name__ == "__main__":
    run()
