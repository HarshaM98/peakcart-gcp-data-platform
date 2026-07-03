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
