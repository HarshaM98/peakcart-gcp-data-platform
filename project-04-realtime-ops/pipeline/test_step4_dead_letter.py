"""
Unit tests for ParseAndValidate in step4_dead_letter.py.

Calls process() directly rather than running a full Beam pipeline, since
ParseAndValidate has no pipeline-level state (windowing, side inputs) --
a plain DoFn method call is enough to exercise its branching logic.
"""

import json
import unittest

import apache_beam as beam

from step4_dead_letter import ParseAndValidate


class ParseAndValidateTest(unittest.TestCase):
    def setUp(self):
        self.dofn = ParseAndValidate()

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

        self.assertEqual(len(results), 1)
        tagged = results[0]
        self.assertIsInstance(tagged, beam.pvalue.TaggedOutput)
        self.assertEqual(tagged.tag, "malformed")
        self.assertIn("missing_fields", tagged.value["error_reason"])
        self.assertIn("warehouse_zone", tagged.value["error_reason"])

    def test_bad_json_is_tagged_malformed(self):
        results = self.run_process(b"{not valid json")

        self.assertEqual(len(results), 1)
        tagged = results[0]
        self.assertIsInstance(tagged, beam.pvalue.TaggedOutput)
        self.assertEqual(tagged.tag, "malformed")
        self.assertIn("invalid_json", tagged.value["error_reason"])
        self.assertEqual(tagged.value["raw_payload"], "{not valid json")

    def test_bad_timestamp_is_tagged_malformed(self):
        event = {
            "order_id": "O12345",
            "customer_id": "C1000",
            "event_type": "placed",
            "warehouse_zone": "zone_a",
            "timestamp": "not-a-timestamp",
        }
        results = self.run_process(json.dumps(event).encode("utf-8"))

        self.assertEqual(len(results), 1)
        tagged = results[0]
        self.assertIsInstance(tagged, beam.pvalue.TaggedOutput)
        self.assertEqual(tagged.tag, "malformed")
        self.assertIn("invalid_timestamp", tagged.value["error_reason"])


if __name__ == "__main__":
    unittest.main()
