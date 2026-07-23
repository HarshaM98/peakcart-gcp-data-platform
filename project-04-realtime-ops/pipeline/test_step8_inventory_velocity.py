"""
Unit tests for step8_inventory_velocity.py's new inventory aggregation
logic: the dedup key (mirroring order_dedup_key, but for inventory events)
and the dedup -> sum pipeline that produces net_quantity_change per
(warehouse_id, product_id).

Runs small in-memory pipelines with DirectRunner rather than hitting
Pub/Sub/BigQuery -- same rationale as test_step7_all_topics.py: this
exercises the transform logic itself, not the I/O.
"""

import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from step8_inventory_velocity import (
    inventory_dedup_key,
    key_by_warehouse_and_product,
    take_first,
)


class InventoryDedupKeyTest(unittest.TestCase):
    def test_identical_events_share_a_key(self):
        event_a = {
            "warehouse_id": "WH_BOSTON",
            "product_id": "P1000",
            "event_type": "received",
            "quantity_change": 10,
            "timestamp": "2026-07-23T10:00:00Z",
        }
        duplicate_of_a = dict(event_a)

        key_a, _ = inventory_dedup_key(event_a)
        key_dup, _ = inventory_dedup_key(duplicate_of_a)

        self.assertEqual(key_a, key_dup)

    def test_different_event_type_is_a_different_key(self):
        received = {
            "warehouse_id": "WH_BOSTON",
            "product_id": "P1000",
            "event_type": "received",
            "quantity_change": 10,
            "timestamp": "2026-07-23T10:00:00Z",
        }
        picked = {**received, "event_type": "picked", "quantity_change": -3}

        key_received, _ = inventory_dedup_key(received)
        key_picked, _ = inventory_dedup_key(picked)

        self.assertNotEqual(key_received, key_picked)


class DedupThenSumPipelineTest(unittest.TestCase):
    """Proves the dedup-then-sum chain used in run(): a duplicate event is
    collapsed before it reaches the sum, so it contributes to
    net_quantity_change exactly once, not twice."""

    def test_duplicate_event_is_not_double_counted(self):
        received = {
            "warehouse_id": "WH_BOSTON",
            "product_id": "P1000",
            "event_type": "received",
            "quantity_change": 10,
            "timestamp": "2026-07-23T10:00:00Z",
        }
        duplicate_of_received = dict(received)
        picked = {
            "warehouse_id": "WH_BOSTON",
            "product_id": "P1000",
            "event_type": "picked",
            "quantity_change": -3,
            "timestamp": "2026-07-23T10:00:05Z",
        }
        other_product = {
            "warehouse_id": "WH_BOSTON",
            "product_id": "P2000",
            "event_type": "received",
            "quantity_change": 100,
            "timestamp": "2026-07-23T10:00:10Z",
        }

        inputs = [received, duplicate_of_received, picked, other_product]

        with TestPipeline() as pipeline:
            result = (
                pipeline
                | beam.Create(inputs)
                | beam.Map(inventory_dedup_key)
                | beam.GroupByKey()
                | beam.Map(take_first)
                | beam.Map(key_by_warehouse_and_product)
                | beam.CombinePerKey(sum)
            )

            # received (10) deduped to one copy, + picked (-3) = 7 for P1000;
            # other_product (100) untouched for P2000. If dedup failed, the
            # duplicate 'received' would push P1000's total to 17.
            assert_that(
                result,
                equal_to([
                    (("WH_BOSTON", "P1000"), 7),
                    (("WH_BOSTON", "P2000"), 100),
                ]),
            )


if __name__ == "__main__":
    unittest.main()
