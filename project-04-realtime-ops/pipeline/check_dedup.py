import apache_beam as beam

from step7_dedup_late_data import dedup_key

event_a = {"order_id": "O1", "event_type": "placed", "timestamp": "2026-07-14T17:00:00Z"}
duplicate_of_a = dict(event_a)
event_b = {"order_id": "O1", "event_type": "packed", "timestamp": "2026-07-14T17:05:00Z"}

inputs = [event_a, duplicate_of_a, event_b]

print(f"INPUT ({len(inputs)} events):")
for e in inputs:
    print(f"  {e}")


def take_first(kv):
    key, events = kv
    return next(iter(events))


with beam.Pipeline() as pipeline:
    (
        pipeline
        | beam.Create(inputs)
        | beam.Map(dedup_key)
        | beam.GroupByKey()
        | beam.Map(take_first)
        | beam.Map(lambda e: print(f"OUTPUT: {e}"))
    )