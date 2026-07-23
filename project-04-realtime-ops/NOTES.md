# Project 4 Study Log

Running notes on what was built, why, and what's worth remembering. Not documentation — see the project README for that.

---

## 2026-07-03 - Repo-wide CLAUDE.md added

**What I built/changed:**
Added a root-level `CLAUDE.md` documenting commands and architecture across all six projects in the repo, including project-04's Pub/Sub → Beam streaming pipeline.

**Why this approach:**
Future Claude Code sessions need enough context to be productive without re-deriving the shared dbt project layout, the stage-and-replace Bronze pattern, and project-04's simulator/pipeline structure from scratch each time.

**Key concept to remember:**
Project-04's Terraform computes the Pub/Sub service agent email directly from `data.google_project.current.number` instead of relying on `google_project_service_identity`'s output, because the latter is unknown until apply and was forcing IAM bindings to be destroyed/recreated on every run.

**Gotchas/issues hit:**
None.

---

## 2026-07-03 - Dead-letter handling for malformed events (step4_dead_letter.py)

**What I built/changed:**
Added `pipeline/step4_dead_letter.py`, replacing step3's defensive `.get("warehouse_zone", "unknown_zone")` fallback with real validation. A single `ParseAndValidate` DoFn now checks JSON parsing, required fields, and timestamp format, using Beam's `TaggedOutput`/`.with_outputs()` multi-output pattern to split malformed events into a `malformed` branch while good events keep flowing through the existing windowed per-zone count.

**Why this approach:**
Chose a BigQuery error table (`malformed_events`) over a GCS dead-letter bucket: the goal is querying/dashboarding malformed-event rates by reason and zone, which BigQuery gives for free, whereas GCS would need a batch load step first. This is a different failure domain than the existing Pub/Sub-level DLQ (which only catches delivery/ack failures) — validation errors like a missing field happen after successful delivery, inside the Beam pipeline, so they need a pipeline-level side output, not the Pub/Sub DLQ.

**Key concept to remember:**
A `beam.Map` that throws crashes the pipeline — it doesn't automatically route to a side output. To dead-letter cleanly, parsing and validation have to happen inside one `DoFn.process()` with explicit try/except around each failure mode, yielding `beam.pvalue.TaggedOutput('malformed', ...)` on failure and returning early so a bad record never also flows to the main output.

**Gotchas/issues hit:**
`warehouse_zone` is optional in the JSON Schema for `order_event` but effectively required for this pipeline's zone-based windowing — added it to `REQUIRED_FIELDS` for pipeline-level validation even though the event schema itself doesn't mandate it.

---

## 2026-07-03 - End-to-end test of step4_dead_letter.py against live GCP

**What I built/changed:**
Added `pipeline/test_step4_dead_letter.py` (unit tests calling `ParseAndValidate.process()` directly for valid/missing-zone/bad-JSON/bad-timestamp cases — all 4 pass) and ran a live end-to-end test: `event_simulator.py --duration 1 --messiness 0.6` against the real `peakcart-order-events` topic, then `step4_dead_letter.py --runner=DirectRunner` against the real subscription, writing to the real `malformed_events` BigQuery table.

**Why this approach:**
Unit tests alone can't catch integration issues like a wrong dataset name — while writing the test I found `MALFORMED_EVENTS_TABLE` pointed at `peakcart_realtime`, a dataset that doesn't exist (the real one is `peakcart_streaming`, discovered via `bq ls`). Fixed before running live.

**Key concept to remember:**
The malformed-events branch has no windowing, so it wrote rows to BigQuery immediately and continuously as messages were consumed — but the good-events branch (`FixedWindows(60)` with the default trigger) printed zero windowed counts in ~2 minutes of DirectRunner runtime. This is a known DirectRunner limitation: its watermark heuristic for an unbounded Pub/Sub source advances slowly/unpredictably, so windows may not close in a short local test even though the same code would behave correctly on Dataflow. Not a regression from this change — step3 would show the same gap.

**Gotchas/issues hit:**
Of 260 order events published, only `missing_fields: ['warehouse_zone']` ever appeared in `malformed_events` (61 rows) — no `invalid_json` or `invalid_timestamp` rows. This is expected: `event_simulator.py`'s messiness model only ever drops the optional `warehouse_zone`/`items_count` fields or staggers timestamps into the past (still valid ISO strings); it never emits malformed JSON or a genuinely unparseable timestamp. Those two code paths are covered only by the unit tests, not by this live run.

**Note — intentional test data in `malformed_events`:** The 61 rows from this 2026-07-03 test run were deliberately left in the live `peakcart_streaming.malformed_events` table (not deleted) as evidence the dead-letter path works end-to-end. All 61 rows have `error_reason = "missing_fields: ['warehouse_zone']"` and `processing_time` around 2026-07-03 17:02 UTC. If this table is ever inspected and these rows look like an unexplained data-quality issue, they are not — they're expected test output from this run, not real production traffic.

---

## 2026-07-03 - Write orders_per_minute to BigQuery (step5_write_to_bigquery.py)

**What I built/changed:**
Added `pipeline/step5_write_to_bigquery.py`, building on step4: replaced the well-formed branch's `Print` placeholder with `WriteToBigQuery` into the existing `orders_per_minute` table, and added the `pipeline_processed_at` field to `FormatWindowedCount`'s output since the table's schema requires it. The malformed branch is unchanged from step4.

**Why this approach:**
This was the last explicitly deferred piece from step3's docstring ("still printing results directly, no BigQuery write yet"). Both branches now write to real BigQuery tables using the same `WriteToBigQuery` pattern, so the pipeline's two outputs are handled symmetrically instead of one going to BigQuery and the other only to stdout.

**Key concept to remember:**
A schema mismatch here would only surface at write time, not at compile time — `FormatWindowedCount` had been silently missing `pipeline_processed_at` since step3 because nothing was writing its output to a schema-enforced sink yet. Adding a real `WriteToBigQuery` call is what exposed the gap.

**Gotchas/issues hit:**
None yet — this hasn't been run live against Pub/Sub/BigQuery yet, only syntax-checked with `py_compile`.

---

## 2026-07-03 - End-to-end test of step5_write_to_bigquery.py against live GCP

**What I built/changed:**
Ran `event_simulator.py --duration 1 --messiness 0.6` (258 order events published), then `step5_write_to_bigquery.py --runner=DirectRunner` for ~3 minutes against the real subscription, then verified both output tables in BigQuery.

**Why this approach:**
Same live end-to-end method used for step4 — DirectRunner against real Pub/Sub/BigQuery, since schema mismatches and dataset/table typos only surface at actual write time, not from `py_compile` or unit tests.

**Key concept to remember:**
Unlike the step4 test run, this run's watermark advanced far enough for 2 windows to close: `orders_per_minute` got 2 rows (zone_c and zone_b, `order_count = 1` each), each with a correctly populated `pipeline_processed_at`, confirming the schema fix (adding that field to `FormatWindowedCount`) was correct and the `WriteToBigQuery` write for the well-formed branch works end-to-end. This is the same DirectRunner watermark heuristic from the step4 test — it's timing-dependent, not deterministic, so a short local run isn't guaranteed to produce output on every run.

**Gotchas/issues hit:**
None. `malformed_events` grew from 61 to 72 rows (11 new, all `missing_fields: ['warehouse_zone']`), confirming the dead-letter branch still works unchanged from step4.

---

## 2026-07-22 - Generalize to all three topics (step6_all_topics.py)

**What I built/changed:**
Added `pipeline/step6_all_topics.py`, extending step5 to read from all three Pub/Sub subscriptions (order, delivery, inventory), not just orders. `ParseAndValidate` became parameterized (`required_fields`, `subscription_name` passed into the constructor) instead of hardcoded for orders, so one DoFn class serves all three event types. Order events are unchanged behavior (validated, windowed, counted per zone, written to `orders_per_minute`). Delivery and inventory events are validated only — well-formed events are printed, not yet aggregated. All three malformed branches are combined via `beam.Flatten()` into the same `malformed_events` table.

**Why this approach:**
Sharing one parameterized `ParseAndValidate` class across topics avoids three near-identical copies of the same validate-or-tag-malformed logic — only the required-fields list and the subscription name (for error attribution) differ per event type. Combining all three malformed branches into one BigQuery sink means error rates can be queried across all event types in one place, rather than three separate tables.

**Key concept to remember:**
Delivery and inventory aggregation (`avg_pick_time`, `active_deliveries`, `inventory_velocity`) is deliberately deferred, not an oversight — each needs different, more complex logic than a simple per-key count (pairing related events across a delivery's lifecycle, session windows), so it doesn't fit the same windowed-count pattern used for orders.

**Gotchas/issues hit:**
None.

---

## 2026-07-22 - Deduplication and late data handling (step7_dedup_late_data.py)

**What I built/changed:**
Added `pipeline/step7_dedup_late_data.py`, building on step6. The order branch now dedups events (key = `order_id + event_type + timestamp`, via `beam.Map` → `GroupByKey` → take-first-per-key) and handles late data (`FixedWindows(60)` with `trigger=AfterWatermark(late=AfterCount(1))`, `accumulation_mode=ACCUMULATING`, `allowed_lateness=300s`). Delivery and inventory branches are unchanged from step6 (still validate-and-print only). Also added `pipeline/check_dedup.py`, a small standalone script that runs the dedup key/GroupByKey/take-first logic against three in-memory events (two exact duplicates, one distinct) to sanity-check the dedup behavior outside the full streaming pipeline. Renamed/moved the step4 unit tests to `pipeline/test_step7_all_topics.py`, importing from `step7_dedup_late_data` and adding new test classes for delivery and inventory validation (proving the now-shared `ParseAndValidate` class correctly validates each event type's own required fields).

**Why this approach:**
`order_id` alone is the wrong dedup key — one order legitimately produces multiple distinct events (placed, picked, packed...) that share an `order_id` but must not be collapsed together. The correct idempotency key is the combination that identifies one specific event occurrence: `order_id + event_type + timestamp`. This matches how `event_simulator.py` actually produces duplicates — it resends the exact same event dict unchanged (see `run_producer`'s `last_event` resend), so a duplicate is byte-for-byte identical under this key, not just related.

The simulator's `stale_iso()` deliberately backdates event timestamps by up to a few minutes to simulate late/out-of-order delivery. Since Beam windows by event time, a stale event can arrive after its window has already closed and fired. `allowed_lateness` (set to 5 minutes to match the simulator's worst case) keeps the window open long enough to accept it, and `ACCUMULATING` mode means a late arrival re-fires the window with an updated cumulative count instead of the event being dropped or producing a confusing separate row for the same window.

**Key concept to remember:**
Dedup has to happen *inside* the window (after `WindowInto`, before `GroupByKey`) so that `beam.GroupByKey()` groups within a single window's contents — grouping before windowing would collapse duplicates across window boundaries, not within the query semantics we want.

**Gotchas/issues hit:**
None yet — not run live against Pub/Sub/BigQuery in this session; verified via unit tests (`test_step7_all_topics.py`) and the standalone `check_dedup.py` script only.
