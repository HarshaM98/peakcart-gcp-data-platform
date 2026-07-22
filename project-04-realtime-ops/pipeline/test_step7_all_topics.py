"""
Unit tests for ParseAndValidate, now living in step7_dedup_late_data.py
(originally step4_dead_letter.py, generalized in step6 to take
required_fields/subscription_name as constructor args so the same class
serves all three event types).

Calls process() directly rather than running a full Beam pipeline, since
ParseAndValidate has no pipeline-level state (windowing, side inputs) --
a plain DoFn method call is enough to exercise its branching logic.
"""

import json
import unittest

import apache_beam as beam

from step7_dedup_late_data import (
    ParseAndValidate,
    ORDER_REQUIRED_FIELDS,
    DELIVERY_REQUIRED_FIELDS,
    INVENTORY_REQUIRED_FIELDS,
)


class ParseAndValidateOrderTest(unittest.TestCase):
    """Same coverage as the original step4 tests, now against the current
    file and constructor signature."""

    def setUp(self):
        self.dofn = ParseAndValidate(ORDER_REQUIRED_FIELDS, "orders-sub")

    def run_process(self, raw_bytes):
        return list(self.dofn.process(raw_bytes))

    def test_valid_event_goes_to_main_output(self):
        event = {
            "order_id": "O12345",
            "customer_id": "C1000",
            "event_type": "placed",
            "warehouse_zone": "zone_a",
            "timestamp": "2026-07-03T10:00:00Z",
            "items_count": 3,
        }
        results = self.run_process(json.dumps(event).encode("utf-8"))

        self.assertEqual(len(results), 1)
        self.assertNotIsInstance(results[0], beam.pvalue.TaggedOutput)
        self.assertEqual(results[0], event)

    def test_missing_warehouse_zone_is_tagged_malformed(self):
        event = {
            "order_id": "O12345",
            "customer_id": "C1000",
            "event_type": "placed",
            "timestamp": "2026-07-03T10:00:00Z",
        }
        results = self.run_process(json.dumps(event).encode("utf-8"))

        tagged = results[0]
        self.assertIsInstance(tagged, beam.pvalue.TaggedOutput)
        self.assertEqual(tagged.tag, "malformed")
        self.assertIn("warehouse_zone", tagged.value["error_reason"])

    def test_bad_json_is_tagged_malformed(self):
        results = self.run_process(b"{not valid json")

        tagged = results[0]
        self.assertIsInstance(tagged, beam.pvalue.TaggedOutput)
        self.assertIn("invalid_json", tagged.value["error_reason"])

    def test_bad_timestamp_is_tagged_malformed(self):
        event = {
            "order_id": "O12345",
            "customer_id": "C1000",
            "event_type": "placed",
            "warehouse_zone": "zone_a",
            "timestamp": "not-a-timestamp",
        }
        results = self.run_process(json.dumps(event).encode("utf-8"))

        tagged = results[0]
        self.assertIsInstance(tagged, beam.pvalue.TaggedOutput)
        self.assertIn("invalid_timestamp", tagged.value["error_reason"])


class ParseAndValidateDeliveryTest(unittest.TestCase):
    """New: proves the same class correctly validates a completely
    different event shape, using delivery's own required fields."""

    def setUp(self):
        self.dofn = ParseAndValidate(DELIVERY_REQUIRED_FIELDS, "delivery-sub")

    def test_valid_delivery_event_passes(self):
        event = {
            "delivery_id": "D1000",
            "driver_id": "DR100",
            "truck_id": "T01",
            "event_type": "departed",
            "timestamp": "2026-07-14T17:00:00Z",
        }
        results = list(self.dofn.process(json.dumps(event).encode("utf-8")))
        self.assertNotIsInstance(results[0], beam.pvalue.TaggedOutput)

    def test_missing_delivery_id_is_malformed(self):
        event = {
            "driver_id": "DR100",
            "truck_id": "T01",
            "event_type": "departed",
            "timestamp": "2026-07-14T17:00:00Z",
        }
        results = list(self.dofn.process(json.dumps(event).encode("utf-8")))
        tagged = results[0]
        self.assertIsInstance(tagged, beam.pvalue.TaggedOutput)
        self.assertIn("delivery_id", tagged.value["error_reason"])


class ParseAndValidateInventoryTest(unittest.TestCase):
    """New: same idea for inventory's required fields."""

    def setUp(self):
        self.dofn = ParseAndValidate(INVENTORY_REQUIRED_FIELDS, "inventory-sub")

    def test_valid_inventory_event_passes(self):
        event = {
            "warehouse_id": "WH_BOSTON",
            "product_id": "P1000",
            "event_type": "received",
            "quantity_change": 10,
            "timestamp": "2026-07-14T17:00:00Z",
        }
        results = list(self.dofn.process(json.dumps(event).encode("utf-8")))
        self.assertNotIsInstance(results[0], beam.pvalue.TaggedOutput)

    def test_missing_quantity_change_is_malformed(self):
        event = {
            "warehouse_id": "WH_BOSTON",
            "product_id": "P1000",
            "event_type": "received",
            "timestamp": "2026-07-14T17:00:00Z",
        }
        results = list(self.dofn.process(json.dumps(event).encode("utf-8")))
        tagged = results[0]
        self.assertIsInstance(tagged, beam.pvalue.TaggedOutput)
        self.assertIn("quantity_change", tagged.value["error_reason"])


if __name__ == "__main__":
    unittest.main()