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
