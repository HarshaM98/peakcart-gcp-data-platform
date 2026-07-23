"""
Unit tests for step9_active_deliveries.py's new delivery aggregation
logic: take_latest_by_timestamp (picks a delivery's current status among
several events, and absorbs exact duplicates for free) and the full
group-by-delivery -> latest -> filter -> count chain used in run().

Runs small in-memory pipelines with DirectRunner rather than hitting
Pub/Sub/BigQuery -- same rationale as test_step7/test_step8: this
exercises the transform logic itself, not the I/O.
"""

import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from step9_active_deliveries import (
    is_not_completed,
    key_by_delivery_id,
    take_latest_by_timestamp,
)


class TakeLatestByTimestampTest(unittest.TestCase):
    def test_picks_the_later_of_two_distinct_events(self):
        departed = {
            "delivery_id": "D1",
            "event_type": "departed",
            "timestamp": "2026-07-23T10:00:00Z",
        }
        in_transit = {
            "delivery_id": "D1",
            "event_type": "in_transit",
            "timestamp": "2026-07-23T10:00:30Z",
        }

        latest = take_latest_by_timestamp(("D1", [departed, in_transit]))

        self.assertEqual(latest["event_type"], "in_transit")

    def test_exact_duplicate_does_not_change_the_result(self):
        arrived = {
            "delivery_id": "D1",
            "event_type": "arrived",
            "timestamp": "2026-07-23T10:05:00Z",
        }
        duplicate_of_arrived = dict(arrived)

        latest = take_latest_by_timestamp(("D1", [arrived, duplicate_of_arrived]))

        self.assertEqual(latest, arrived)


class IsNotCompletedTest(unittest.TestCase):
    def test_completed_is_filtered_out(self):
        self.assertFalse(is_not_completed({"event_type": "completed"}))

    def test_other_stages_pass_through(self):
        for stage in ["departed", "in_transit", "arrived"]:
            self.assertTrue(is_not_completed({"event_type": stage}))


class ActiveDeliveriesPipelineTest(unittest.TestCase):
    """Proves the group-by-delivery -> latest -> filter -> count chain used
    in run() counts distinct in-flight deliveries, not raw events, and
    excludes completed ones."""

    def test_counts_only_distinct_non_completed_deliveries(self):
        events = [
            # D1: departed then in_transit -- latest is in_transit, active.
            {"delivery_id": "D1", "event_type": "departed",
             "timestamp": "2026-07-23T10:00:00Z"},
            {"delivery_id": "D1", "event_type": "in_transit",
             "timestamp": "2026-07-23T10:00:30Z"},
            # D2: only completed -- not active.
            {"delivery_id": "D2", "event_type": "completed",
             "timestamp": "2026-07-23T10:00:10Z"},
            # D3: arrived, plus an exact duplicate -- still one active delivery.
            {"delivery_id": "D3", "event_type": "arrived",
             "timestamp": "2026-07-23T10:00:20Z"},
            {"delivery_id": "D3", "event_type": "arrived",
             "timestamp": "2026-07-23T10:00:20Z"},
        ]

        with TestPipeline() as pipeline:
            result = (
                pipeline
                | beam.Create(events)
                | beam.Map(key_by_delivery_id)
                | beam.GroupByKey()
                | beam.Map(take_latest_by_timestamp)
                | beam.Filter(is_not_completed)
                | beam.combiners.Count.Globally().without_defaults()
            )

            # D1 (in_transit) and D3 (arrived) are active; D2 (completed) is not.
            assert_that(result, equal_to([2]))


if __name__ == "__main__":
    unittest.main()
