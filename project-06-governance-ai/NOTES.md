# Project 6 — Build Notes

Dated build log, same convention as project-04/project-05: what was built, why
that approach, the key concept worth remembering, and gotchas actually hit.

---

## 2026-08-02 — Scoping: Dataplex DQ + a real Gemini API integration, not a rebuild of Gemini in BigQuery

**What I decided:**
Originally scoped "GenAI workflows" as either a natural-language-to-SQL assistant or
auto-generated table/column documentation. Both are already shipped as first-party
console features ("Gemini in BigQuery" does NL2SQL and suggests column
descriptions natively) — building a custom version of either would just be
reinventing a button that already exists, not demonstrating anything.

Repointed the GenAI piece at something Google's console feature doesn't do:
call the Gemini API *programmatically* from a custom governance workflow — a
Dataplex data quality scan whose results get turned into a plain-English report
by Gemini, rather than a person reading raw pass/fail percentages.

**Why this approach:**
The transferable skill here is calling the Generative AI API and wiring it into
a pipeline, not knowing a console feature exists. Grounding it in Dataplex data
quality scanning also gives project-06 a genuine "data governance" backbone
consistent with its portfolio slot (Dataplex, IAM, GenAI workflows).

---

## 2026-08-02 — Data quality rule design: read the actual dbt SQL first, found the gold layer isn't as clean as it looks

**What I found:**
Rather than assume the gold (mart) layer is clean and scan it for form's sake,
read `project-01-data-warehouse`'s actual staging/mart SQL first:

- `stg_order_items.sql` computes `is_positive_quantity` / `is_positive_price` /
  `is_valid_discount` quality flags in staging — but `fact_orders.sql`'s select
  list never carries those flags forward, and there's no `WHERE` filter on
  them either. Negative-quantity/invalid-discount rows seeded in the raw data
  flow silently into the gold fact table today, unflagged.
- `stg_products.sql` casts `supplier_id` with no nullness flag and no filter —
  NULL-supplier rows can sit in `dim_products` unflagged.
- `dim_customers.sql`, by contrast, *does* correctly carry `is_valid_email` /
  `is_valid_signup_date` through as real gold columns (an existing flag,
  just never queried/monitored by anything).

**Why this approach:**
This turns project-06 into "governance scanning catches what dbt's own
staging layer already computed but silently dropped at the mart" rather than
a scan on data assumed clean. A concrete, defensible finding beats a
manufactured one.

**Data quality rules built** (Dataplex `DataScan`, one per gold table,
threshold 1.0 so any real violation shows as FAILED rather than being
tolerance-absorbed):

| Table | Rule | Dimension |
| --- | --- | --- |
| `fact_orders` | `quantity > 0` | VALIDITY |
| `fact_orders` | `unit_price > 0` | VALIDITY |
| `fact_orders` | `discount BETWEEN 0 AND 1` | VALIDITY |
| `fact_orders` | `customer_surrogate_key IS NOT NULL` | COMPLETENESS |
| `fact_orders` | `product_surrogate_key IS NOT NULL` | COMPLETENESS |
| `dim_customers` | `is_valid_email = true` | VALIDITY |
| `dim_customers` | `customer_surrogate_key` uniqueness | UNIQUENESS |
| `dim_products` | `supplier_id IS NOT NULL` | COMPLETENESS |
| `dim_products` | `product_surrogate_key` uniqueness | UNIQUENESS |
| `fact_daily_inventory` | `qty_available >= 0` | VALIDITY |

**Real results from the first live run** (confirms the hypothesis, not a
contrived demo):

- `fact_orders`: **FAILED** on `quantity > 0` — 55 of 14,847 rows (0.37%)
- `dim_customers`: **FAILED** on `is_valid_email` — 21 of 1,000 rows (2.10%,
  matches the documented ~2% seeded NULL-email rate)
- `dim_products`: **FAILED** on `supplier_id IS NOT NULL` — 12 of 359 rows
  (3.34%)
- `fact_daily_inventory`: passed every rule (a genuine negative result —
  this business-logic bound is never actually violated)
- All other rules on all tables: passed 100%

---

## 2026-08-02 — Terraform: Dataplex lake/zone/asset + DataScans, discovered Eventarc has no scan-completion event

**What I built:**
`infrastructure/terraform/main.tf`: enabled `dataplex.googleapis.com`, created
a `google_dataplex_lake` (`peakcart-governance-lake`) with a `CURATED` zone
(`gold-curated-zone`), registered the *existing* `peakcart_gold` BigQuery
dataset (built in project-01) as a Dataplex asset — reused as-is, not
duplicated. Added 4 `google_dataplex_datascan` resources (data quality type,
on-demand trigger) implementing the rule set above.

**Key concept to remember (checked before building, not after):**
Originally planned "Cloud Function fires automatically when a scan
completes" via Eventarc. Checked directly with
`gcloud eventarc providers describe dataplex.googleapis.com --location=us-central1`
rather than assuming — Dataplex's only direct Eventarc events are
`dataScan.v1.{created,updated,deleted}` (the *resource* lifecycle), not
job/execution completion. Also checked BigQuery: `gcloud eventarc providers
list` shows no direct BigQuery provider at all — only Cloud-Audit-Log-based
triggers exist there, and those fire on job *submission*, not completion.
Pivoted the design before writing any function code: a single self-contained
Cloud Function that runs the scan, polls it to completion, then calls Gemini
— invoked by Cloud Scheduler rather than reacting to an event that doesn't
exist. Same "verify the platform actually supports X before building on top
of it" discipline used everywhere else in this portfolio (the hidden tenant
training SA in project-05, the DirectRunner Session-window limitation in
project-04).

**Terraform schema gotchas hit:**
`google_dataplex_lake`/`zone`/`asset` use the attribute name `name`, not
`lake_id`/`zone_id`/`asset_id` as I first guessed by analogy with other
Google provider resources — caught immediately by `terraform validate`
rather than an apply failure.

---

## 2026-08-02 — Cloud Function: three real bugs stacked, found by working outward from "does this piece even work"

**What I built:**
`functions/anomaly_explainer/main.py` — an HTTP Cloud Function (2nd gen) that
runs all 4 DataScans, polls each to completion via the Dataplex REST API,
summarizes the rule results, and calls Gemini to generate a plain-English
report. Deployed via Terraform (`google_cloudfunctions2_function`), with a
dedicated service account (`peakcart-governance-function`, granted
`dataplex.dataScanAdmin` + BigQuery roles + `aiplatform.user`) and a bounded,
**paused-by-default** `google_cloud_scheduler_job` (cost discipline — created
so the automation story is real and Terraform-defined, not left running
unattended).

**Three real, stacked bugs found during live verification (not one):**

1. **Wrong/expired model name.** `gemini-2.0-flash-001` returned `404
   NOT_FOUND` — checked directly which models actually resolve
   (`gemini-2.5-flash` / `gemini-2.5-pro` do) rather than guessing again.
   Also hit a deprecation warning on `vertexai.generative_models` pointing at
   the newer `google-genai` SDK — switched to `genai.Client(vertexai=True,
   ...)` instead of patching the deprecated path.
2. **`pip` dependency conflicts, twice in a row.** First `google-auth==2.35.0`
   pinned below what `google-cloud-aiplatform` required; after switching to
   `google-genai`, the *same* pin conflicted again (`google-genai` needs
   `google-auth>=2.56.0`). Fixed by dropping the exact pin in favor of a
   floor (`google-auth>=2.56.0`) rather than chasing exact cross-package
   version numbers.
3. **The real one: sequential scans genuinely take longer than the default
   300s function timeout.** Every clean invocation attempt returned a `504
   upstream request timeout` with **zero** application log lines, even after
   confirming (via a completely fresh, from-scratch-recreated function) that
   it wasn't a wedged-instance artifact. Isolated the cause methodically:
   tested `_access_token()` + a single Dataplex `:run` call in isolation
   (fast, correct), then the full 4-scan sequential poll loop using a
   throwaway "hello world"-style test function with the same service
   account and dependencies, stripped of Gemini entirely, deployed with a
   longer timeout purely to observe real behavior. That confirmed each scan
   was taking **~90-100s** end-to-end in practice (vs. ~30s seen in earlier,
   isolated manual testing — almost certainly BigQuery on-demand slot
   contention from the volume of test scans run during this same session,
   not a Cloud Run/Dataplex defect). Fixed by raising `timeout_seconds` to
   480 and `available_cpu` to `"1"` (the default low CPU allocation was a
   secondary suspect, ruled out by testing a `requests.Session()` reuse fix
   in isolation first — that alone did *not* fix it, confirming the real
   bottleneck was scan duration, not per-request connection overhead).

**Why this diagnostic approach:**
Rather than keep guessing at the deployed function, built the smallest
possible reproduction at each step (bare token fetch -> single Dataplex call
-> full 4-scan loop, all under the *same* real service account) using
disposable throwaway functions, so each fix was verified against evidence
before touching the real Terraform-managed resource again. This is the same
"local reproduction, not blind redeploys" instinct as testing project-05's
pipeline components with the BigQuery client directly instead of trusting
opaque component output.

**Result once fixed:** a real, live Gemini-generated governance report
correctly explaining all three real anomalies (55 negative-quantity orders,
21 invalid emails, 12 missing supplier IDs) plus the one clean table, in
plain English with real-world-impact framing — not templated boilerplate.

**Gotchas/issues hit:**
`ModelDeploymentMonitoringJob`-style teardown-before-delete rules also apply
here in a different form: nothing analogous was needed for the Cloud
Function/Scheduler (both delete cleanly without a pause step), but the two
disposable diagnostic test functions (`peakcart-hello-test`,
`peakcart-hello-test2`) were deleted immediately once their job was done, to
avoid leaving stray unmanaged (non-Terraform) resources behind.

---

## 2026-08-02 — Verification and teardown

Invoked the real, Terraform-managed function directly (authenticated
`curl` with an identity token, matching the pattern used for Vertex AI
endpoint testing in project-05). Confirmed the full chain live: DataScans ran
for real, results matched the rules above, and Gemini produced a correct,
non-fabricated report referencing the actual failing row counts.

Torn down afterward: the Cloud Function, its two `run.invoker` IAM bindings,
and the (already-paused) Cloud Scheduler job — the live serving/compute
surface. Left in place, matching every other project's pattern of keeping
the Terraform-managed deliverable rather than tearing down the whole build:
the Dataplex lake/zone/asset, all 4 DataScans, the two service accounts, and
the function-source GCS bucket — none of these carry an ongoing cost, and
the torn-down pieces are fully reproducible from Terraform on demand.
