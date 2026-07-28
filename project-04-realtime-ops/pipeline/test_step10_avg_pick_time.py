"""
Unit tests for step10_avg_pick_time.py's new order pick-time logic:
ExtractPickTime (pairing placed/picked events per order_id) and
PickTimeStatsFn (avg + count in one combine), plus the full
group-by-order -> extract -> stats chain.

Runs small in-memory pipelines with DirectRunner rather than hitting
Pub/Sub/BigQuery -- same rationale as test_step7/8/9: this exercises the
transform logic itself, not the I/O.
"""

import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from step10_avg_pick_time import ExtractPickTime, PickTimeStatsFn, key_by_order_id


class ExtractPickTimeTest(unittest.TestCase):
    def setUp(self):
        self.dofn = ExtractPickTime()

    def test_placed_and_picked_pair_yields_seconds_between_them(self):
        placed = {"order_id": "O1", "event_type": "placed",
                  "timestamp": "2026-07-23T10:00:00Z"}
        picked = {"order_id": "O1", "event_type": "picked",
                  "timestamp": "2026-07-23T10:00:30Z"}

        results = list(self.dofn.process(("O1", [placed, picked])))

        self.assertEqual([r.value for r in results], [30.0])

    def test_missing_picked_yields_nothing(self):
        placed = {"order_id": "O1", "event_type": "placed",
                  "timestamp": "2026-07-23T10:00:00Z"}
        packed = {"order_id": "O1", "event_type": "packed",
                  "timestamp": "2026-07-23T10:00:45Z"}

        results = list(self.dofn.process(("O1", [placed, packed])))

        self.assertEqual(results, [])

    def test_missing_placed_yields_nothing(self):
        picked = {"order_id": "O1", "event_type": "picked",
                  "timestamp": "2026-07-23T10:00:30Z"}

        results = list(self.dofn.process(("O1", [picked])))

        self.assertEqual(results, [])

    def test_negative_delta_is_discarded(self):
        # Simulates messiness backdating "picked" earlier than "placed".
        placed = {"order_id": "O1", "event_type": "placed",
                  "timestamp": "2026-07-23T10:00:30Z"}
        picked = {"order_id": "O1", "event_type": "picked",
                  "timestamp": "2026-07-23T10:00:00Z"}

        results = list(self.dofn.process(("O1", [placed, picked])))

        self.assertEqual(results, [])

    def test_extra_stages_present_are_ignored(self):
        placed = {"order_id": "O1", "event_type": "placed",
                  "timestamp": "2026-07-23T10:00:00Z"}
        picked = {"order_id": "O1", "event_type": "picked",
                  "timestamp": "2026-07-23T10:00:10Z"}
        packed = {"order_id": "O1", "event_type": "packed",
                  "timestamp": "2026-07-23T10:00:25Z"}

        results = list(self.dofn.process(("O1", [placed, picked, packed])))

        self.assertEqual([r.value for r in results], [10.0])


class PickTimeStatsFnTest(unittest.TestCase):
    def setUp(self):
        self.fn = PickTimeStatsFn()

    def test_computes_average_and_count(self):
        acc = self.fn.create_accumulator()
        for value in [10.0, 20.0, 30.0]:
            acc = self.fn.add_input(acc, value)

        result = self.fn.extract_output(acc)

        self.assertEqual(result, {"avg_pick_time_seconds": 20.0, "sample_count": 3})

    def test_merge_accumulators(self):
        acc_a = self.fn.add_input(self.fn.create_accumulator(), 10.0)
        acc_b = self.fn.add_input(self.fn.create_accumulator(), 30.0)

        merged = self.fn.merge_accumulators([acc_a, acc_b])
        result = self.fn.extract_output(merged)

        self.assertEqual(result, {"avg_pick_time_seconds": 20.0, "sample_count": 2})


class AvgPickTimePipelineTest(unittest.TestCase):
    """Proves the group-by-order -> extract -> stats chain used in run()
    averages only complete placed/picked pairs, ignoring orders that
    never reached "picked"."""

    def test_averages_only_complete_pairs(self):
        events = [
            {"order_id": "O1", "event_type": "placed",
             "timestamp": "2026-07-23T10:00:00Z"},
            {"order_id": "O1", "event_type": "picked",
             "timestamp": "2026-07-23T10:00:10Z"},  # 10s pick time
            {"order_id": "O2", "event_type": "placed",
             "timestamp": "2026-07-23T10:00:00Z"},
            {"order_id": "O2", "event_type": "picked",
             "timestamp": "2026-07-23T10:00:30Z"},  # 30s pick time
            {"order_id": "O3", "event_type": "placed",
             "timestamp": "2026-07-23T10:00:00Z"},  # never picked -- excluded
        ]

        with TestPipeline() as pipeline:
            result = (
                pipeline
                | beam.Create(events)
                | beam.Map(key_by_order_id)
                | beam.GroupByKey()
                | beam.ParDo(ExtractPickTime())
                | beam.CombineGlobally(PickTimeStatsFn()).without_defaults()
            )

            assert_that(
                result,
                equal_to([{"avg_pick_time_seconds": 20.0, "sample_count": 2}]),
            )


if __name__ == "__main__":
    unittest.main()
