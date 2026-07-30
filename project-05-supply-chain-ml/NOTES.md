# Project 5 Study Log

Running notes on what was built, why, and what's worth remembering. Not
documentation — see the project README (once written) for that. Mirrors
the format established in project-04-realtime-ops/NOTES.md.

---

## 2026-07-29 - Phase 1: signal-injected data + Dataform feature table

**What I built/changed:**
Added `shared/data-generators/generate_project05_data.py`, a new, separate generator (not touching `generate_peakcart_data.py`, which projects 1/3 depend on for exact row counts/seed behavior). It reads the already-generated `products.csv`/`suppliers.csv` and simulates a full year of daily demand + inventory per product, with **genuine causal structure** rather than uniformly random fields: demand has weekend seasonality and price elasticity; stock depletes against that demand and reorders after the product's real supplier lead time; stockouts are a real emergent outcome of that simulation, not an independently-random label. Verified concretely before building anything downstream: stockout rate rises monotonically with lead time (3.87% short / 9.36% medium / 14.96% long lead-time buckets) and with recent demand velocity. Loaded the output into a new BigQuery dataset (`peakcart_supply_chain_bronze`, via Terraform reusing project-01's `bigquery_dataset` module) and built a Dataform project (`dataform/`) with source declarations (including reusing project-01's existing `bronze_products` for category/price rather than duplicating it) and a feature table (`peakcart_supply_chain_features.stockout_risk_features`) predicting stockout risk 7 days out.

**Why this approach:**
Started this project because the user correctly pushed back that the *other* generators' fields are deliberately uncorrelated (they exist to test pipeline robustness, not to support ML) — training a model on that would just fit noise, and a careful reviewer would notice immediately (why would a `random.randint` field predict anything?). The real goal here (per the user) is learning how AI/ML is actually deployed on GCP, which requires the underlying data to have real signal, or the "model accuracy" numbers are theatre. First attempt at the safety-stock buffer (`SAFETY_STOCK_FACTOR=1.1`) made stockouts too rare (<1%) and the lead-time signal too weak to be a clean demonstration; retuned to `0.85` (deliberately under-provisioned) and widened demand noise, which produced the clean, strong signal above.

The `stockout_next_7d` label deliberately looks only **forward** (`ROWS BETWEEN 1 FOLLOWING AND 7 FOLLOWING`), never at the same day's own `stockout_flag` — using today's own outcome as a feature (or letting it leak into the window) would make the "prediction" trivial. Rows within 7 days of each product's last date are dropped, since their forward window would be truncated rather than a genuine 7-day horizon.

**Key concept to remember:**
BigQuery dataset IDs are immutable — renaming `peakcart_project05_bronze`/`_features` to `peakcart_supply_chain_bronze`/`_features` (the user didn't want "project05" in a dataset name sitting next to `peakcart_bronze`/`peakcart_streaming`) meant deleting and recreating them, not an in-place rename. Terraform's `bigquery_dataset` module has `delete_contents_on_destroy = false`, so the old dataset had to be dropped manually (`bq rm -r -f`) before Terraform would let a `dataset_id` change (which forces destroy+create) proceed — otherwise `terraform apply` would fail trying to destroy a non-empty dataset.

**Gotchas/issues hit:**
Dataform CLI (`npx @dataform/cli@3`) needs a `.df-credentials.json` file to run, but a minimal one containing just `{"projectId": ..., "location": ...}` was enough — it falls back to Application Default Credentials rather than requiring a service account key file, consistent with this repo's no-inline-credentials lesson from project-03. Modern Dataform (`dataformCoreVersion: 3.x`) uses `workflow_settings.yaml`, not the older `dataform.json` format some docs/muscle-memory expect.

---

## 2026-07-29 - Phase 2: BigQuery ML (CREATE MODEL, ML.EVALUATE, ML.PREDICT)

**What I built/changed:**
Added `dataform/definitions/train_stockout_risk_model.sqlx` (a Dataform `operations` action, so model training is versioned and reproducible alongside the feature table, not a one-off manual query) that trains `peakcart_supply_chain_features.stockout_risk_model`, a `LOGISTIC_REG` BQML model with `auto_class_weights=true`. Trained on Jan-Oct data, held out Nov-Dec entirely for evaluation. `ML.EVALUATE` on that held-out period: **ROC AUC 0.90**, accuracy 83%, F1 0.75. `ML.WEIGHTS` confirmed the model actually learned the intended causal structure: `lead_time_days` (weight 0.120) and `rolling_7d_avg_demand` (0.109) are the two strongest positive drivers of stockout risk, `qty_on_hand` is negative (more stock -> lower risk) — matching the simulation's design exactly, not an accident. `ML.PREDICT` on a sample product showed predicted risk climbing smoothly and monotonically from 19% to 89% as its stock genuinely depleted over 10 days, flipping to a positive prediction right around where the actual label also flips.

**Why this approach:**
Used a **time-based** train/test split (`WHERE date < '2025-11-01'` for training, `>= '2025-11-01'` for eval) rather than a random split. Rows for the same product on nearby days are highly similar (slowly-changing stock level, same lead time), so a random split would let near-duplicate rows leak between train and test and overstate accuracy. A time split mirrors how this model would actually be judged in deployment: can it generalize to a future period it never saw, not just interpolate within a period it partially trained on.

**Key concept to remember:**
In a Dataform `operations` block, `${self()}` already returns the fully-qualified, backtick-quoted table/model reference — wrapping it in another pair of backticks (`` `${self()}` ``) produces broken double-backtick SQL. Also: `hasOutput: true` plus an explicit `name:` in the config lets the actual BigQuery model name differ from the `.sqlx` filename (file is `train_stockout_risk_model.sqlx`, describing the action; the model itself is `stockout_risk_model`, the artifact).

**Gotchas/issues hit:**
`ML.WEIGHTS`'s output shape differs for numeric vs. categorical features — numeric features have a scalar `weight` column, but categorical features (like `category` here) carry their per-category weights in a separate nested/repeated column, so querying `weight` directly for a categorical feature returns `NULL` rather than an error. Not a bug, just a schema nuance worth knowing before assuming a `NULL` means the feature didn't matter.

---

## 2026-07-29 - Phase 3: Vertex AI Model Registry + online endpoint

**What I built/changed:**
Added `model_registry = 'vertex_ai'`, `vertex_ai_model_id = 'stockout-risk-model'`, and `vertex_ai_model_version_aliases = ['default']` to the `CREATE MODEL` options in `train_stockout_risk_model.sqlx` — this is BQML's built-in Vertex AI integration: every time the model retrains, it's automatically registered/updated in the Vertex AI Model Registry, no separate export/upload step needed. Enabled the `aiplatform.googleapis.com` API via Terraform. Created a Vertex AI endpoint (`stockout-risk-endpoint`), deployed the model to it (`n1-standard-2`), and made real online prediction calls three different ways — `curl` against the REST API directly, and the console's own "Deploy & test" UI "Infer" button — all returning identical results (89.4% stockout risk probability for a low-stock/high-lead-time/high-demand example), cross-validating that the model, the deployment, and the serving path are all actually consistent. Verified, screenshotted, then undeployed and deleted the endpoint — nothing is left running/billing.

**Why this approach:**
This is the standard BQML-to-Vertex-AI path precisely because it avoids a manual export/import step — training and registration are the same action. Deploying to a real endpoint (rather than stopping at `ML.PREDICT`, which is BigQuery-side batch/interactive scoring) is what actually demonstrates the "online serving" half of GCP's ML story, which BQML alone doesn't provide.

**Key concept to remember (the explanation-preprocessing failure):**
The first deployment attempt failed after ~13 minutes with `Error occurred in Explanation preprocessing... NodeDef mentions attr 'debug_name' not in Op<VarHandleOp>...` — a TensorFlow graph-version mismatch, not a problem with the model or endpoint config. Root cause: registering a BQML model to Vertex AI auto-attaches a Shapley-value `explanationSpec` (visible via `gcloud ai models describe`), and Vertex's default deployment path tries to build an explanation-serving graph for it, which failed against this model's exported TF graph. The fix — disabling explanations for the deployment — isn't exposed as a `gcloud ai endpoints deploy-model` flag at all; it required calling the Vertex AI REST API directly (`POST .../endpoints/{id}:deployModel` with `"deployedModel": {"disableExplanations": true, ...}`) since the CLI has no equivalent option. `gcloud ai operations describe` also doesn't work for endpoint deploy-model long-running operations (it's scoped to index/index-endpoint operations only) — the actual error had to be pulled via a raw authenticated `curl` against the operation's REST resource, same technique as diagnosing the Composer worker-churn issue in project-04.

**Gotchas/issues hit:**
A second deployment attempt (this one just for taking UI screenshots, after the first successful deploy had already been undeployed) failed with a generic `code 13` "System error, please try again" — a one-off transient Google-side failure, not related to our config, since the identical request succeeded on the very next retry. Worth just retrying once before assuming a real problem when this specific error appears.

---

## 2026-07-29 - Phase 4: Vertex AI Pipeline (train -> evaluate -> conditional deploy)

**What I built/changed:**
Added `pipelines/stockout_risk_pipeline.py`, a KFP v2 pipeline (compiled and submitted via `google-cloud-aiplatform`'s `PipelineJob`, in a dedicated `~/.venv/vertex-pipelines-env`) that automates what was three manual steps in Phases 2-3 into one job: train the BQML model (`BigqueryCreateModelJobOp`, Google's own component), evaluate it on the held-out Nov-Dec period (a custom Python component using the BigQuery client directly), and conditionally deploy it (`dsl.If(roc_auc >= threshold)`) using a custom Python component that calls the Vertex AI REST API directly with `disableExplanations=true` -- reusing the exact fix from the Phase 3 deployment failure, since neither `ModelDeployOp` nor `gcloud ai endpoints deploy-model` expose a way to disable the auto-attached explanation spec. Added a dedicated `peakcart-vertex-pipelines` service account and a `peakcart-vertex-pipelines-2026` GCS bucket (pipeline artifact root, 14-day lifecycle) via Terraform. After two IAM-related failures (below), a full run succeeded end-to-end: trained a fresh model version, evaluated it at ROC AUC 0.9006 (confirmed via the task's actual output value, not just its SUCCEEDED state), took the deploy branch for real, and served a live prediction from the pipeline-deployed endpoint (89.4% risk, identical to every prior manual test). Undeployed and deleted the endpoint after verifying.

**Why this approach:**
`BigqueryEvaluateModelJobOp` (Google's own component for this) outputs an opaque `system.Artifact` whose schema wasn't worth reverse-engineering under time pressure; a custom component using the BigQuery client directly keeps the ROC AUC extraction transparent and makes it a first-class pipeline value the `dsl.If` can compare against numerically. This is the real "why a Vertex AI Pipeline at all" answer from the plan: without it, retraining/evaluating/deploying are three commands a person has to remember to run in order, with no automatic gate stopping a worse model from replacing a better one in production. With this, the gate is code, not memory.

**Key concept to remember (two distinct IAM failures, not one):**
1. The **first run** failed after ~9 minutes with `Access Denied: ... does not have bigquery.jobs.create permission`, because I hadn't specified a `service_account` for the `PipelineJob` at all, so it defaulted to a compute service account with no BigQuery grants. Fixed by creating `peakcart-vertex-pipelines` with `bigquery.jobUser`/`bigquery.dataEditor`/`aiplatform.user`/`storage.objectAdmin` and passing `service_account=` explicitly to `job.submit()`.
2. The **second run** failed the *same way*, even with that service account correctly attached -- because `BigqueryCreateModelJobOp` doesn't actually run the BigQuery job as the pipeline's configured service account. Digging into `cloudaudit.googleapis.com/data_access` logs (`authenticationInfo.principalEmail`) showed the real caller was a **Vertex-AI-managed tenant-project service account** (`training-<id>@<tenant>-tp.iam.gserviceaccount.com`) -- an internal identity Vertex mints per-project for this component's underlying CustomJob launcher, completely separate from the pipeline's own `service_account`. The fix was granting `bigquery.jobUser`/`bigquery.dataEditor` directly to that exact tenant SA. This was not obvious from any error message or the component's documentation -- it only became visible by tracing the actual failing principal in the audit log, the same technique used to diagnose the Composer worker-churn issue in project-04 and the Dataflow explanation-preprocessing failure in this project's Phase 3.

A **third**, separate failure (between fixing #1 and discovering #2) was `Permission 'aiplatform.metadataStores.get' denied ... (or it may not exist)` at pipeline *submission* time, despite the granted role (`roles/aiplatform.user`) actually including that permission (confirmed via `gcloud iam roles describe`). This was IAM propagation delay for the freshly-created service account, not a real permissions gap -- it succeeded on a bare retry a few minutes later with no config change.

**Gotchas/issues hit:**
None beyond the three above -- once both the pipeline's own service account and the hidden tenant training SA had the right BigQuery roles, the run succeeded on the first subsequent attempt.

---

## 2026-07-30 - Pipeline scheduling: caching and retrain-while-deployed both had to be fixed

**What I built/changed:**
Created a Vertex AI `PipelineJobSchedule` (`pipeline_job.create_schedule(cron="* * * * *", max_run_count=1, ...)`) to prove out real recurring-pipeline mechanics without leaving anything running unattended -- `max_run_count=1` means the schedule fires exactly once then permanently transitions to `COMPLETED` (confirmed: `started_run_count=1`, state `COMPLETED`, can never fire again). Along the way, found and fixed two real bugs in the Phase 4 pipeline that only show up on a *second* run against the same parameters -- exactly the scenario a recurring schedule creates and a one-off manual run never would have caught:

1. **Silent full-pipeline caching.** The scheduled run's `bigquery-create-model-job` and `evaluate-model` tasks came back `SKIPPED`, and a subsequent manual run skipped *every* task including the deploy step -- confirmed by diffing the deploy task's output artifact (`endpoints/3842208748646957056`, byte-identical to the prior run) and the endpoint itself (completely unchanged `createTime`/deployed-model ID). Root cause: Vertex AI Pipelines caches a task's execution whenever its inputs match a prior run, **regardless of side effects** -- with fixed default parameter values (the normal shape of a recurring schedule), every run's inputs are identical to the first, so nothing ever actually re-executes. Confirmed via the compiled IR: the old spec showed `"cachingOptions": {"enableCache": true}` explicitly; after calling `.set_caching_options(False)` on all three tasks, the new spec shows `"cachingOptions": {}` -- proto3's way of representing the non-default `enableCache: false` (false is the field default, so it's omitted from JSON rather than explicitly written).
2. **BQML refuses to retrain a deployed model.** With caching fixed, the very next run failed for a completely different, more fundamental reason: `CREATE OR REPLACE MODEL` returned `FAILED_PRECONDITION: The Model is deployed or being deployed at the following Endpoint(s)... Please undeploy the model before retry.` BQML won't let you replace a model resource while it's actively backing a live Vertex AI deployment. Added a new first pipeline step, `undeploy_existing_model`, that finds the target endpoint (if it exists) and undeploys whatever's currently serving there -- *before* training runs -- so retraining is never blocked by the pipeline's own prior successful deployment.

With both fixed, a clean end-to-end run: undeployed the old model (id `4797306121184346112`) -> retrained (genuinely, not cached -- **model version 3**, not still version 2) -> evaluated for real -> deployed version 3 to the *same* reused endpoint (id `7170703124808597504`) -> traffic swapped 100% -> live prediction confirmed correct (89.4% risk, matching every prior test). Undeployed and deleted the endpoint afterward; the schedule was already permanently `COMPLETED` from its one bounded firing.

**Why this approach:**
Deliberately did NOT try to build a zero-downtime blue-green swap (train a new model version under a separate alias, deploy it, then retire the old one only after the new one is confirmed healthy) -- that's the real production-grade fix for the "retrain while serving" conflict, but it's real added complexity this project didn't need to take on just to demonstrate the core MLOps gate (train -> evaluate -> deploy only if good enough). Documenting the trade-off directly in the `undeploy_existing_model` docstring instead of hiding it: this pipeline has a real serving gap during every retrain cycle, by design, not by oversight.

**Key concept to remember:**
Neither of these two bugs was visible from a single successful run -- both `stockout-risk-pipeline-run-4` (Phase 4's original verification) and the first scheduled firing looked completely fine in isolation. They only surfaced on the *second* execution against unchanged parameters, which is precisely the situation a real recurring schedule creates and a one-off demo never exercises. Worth remembering generally: verifying a pipeline is idempotent/safe to rerun is a different (and necessary) test from verifying it works once.

**Gotchas/issues hit:**
None beyond the two described above.

---

## 2026-07-30 - Model Monitoring: config verified live, periodic scheduler stalled (documented limitation)

**What I built/changed:**
Created a `ModelDeploymentMonitoringJob` (`stockout-risk-monitoring-test`) against a fresh endpoint, with `trainingPredictionSkewDetectionConfig` on all 5 features (thresholds 0.1), `randomSampleConfig` at 100% sample rate, `monitorInterval`/`monitorWindow` at 3600s (the 1-hour minimum), and `emailAlertConfig` with `enableLogging: true`. Confirmed the training-side baseline computed correctly (GCS `training/stats_and_anomalies/<model_id>/stats/current_per_feature/` populated for every feature). Sent deliberately skewed prediction traffic directly to the endpoint (`qty_on_hand≈5000`, `rolling_7d_avg_demand≈400`, `price≈999`, all far outside training ranges) and confirmed via BigQuery that `serving_predict` was actually receiving rows (0 -> 1 -> 6 across two rounds of manual requests) -- this also caught and ruled out an initial false alarm where the *first* round of skewed traffic apparently never reached this specific endpoint at all (0 rows persisted for over an hour until I resent it directly and watched the count move).

**Why this approach:**
Same rigor as every other phase: confirm with independent evidence (actual row counts, actual GCS paths, actual log entries), not job/task `state` alone. This is what caught both the "traffic never landed" false alarm and the real issue below.

**Key concept to remember (the real finding -- scheduler stall, not a wait-longer situation):**
After serving traffic was confirmed logged, the periodic serving-side skew analysis never ran, even after 1.5+ hours against a 1-hour `monitorInterval`. Evidence it's a genuine stall rather than normal lag: `nextScheduleTime` and `updateTime` on the job were frozen at the exact same values (`19:00:00Z` / `19:24:23Z`) across every check spanning the full wait, `scheduleState` showed `OFFLINE` even while the job's own `state` showed `JOB_STATE_RUNNING`, no `serving/` GCS directory ever appeared alongside `training/`, no anomaly log entries appeared, and there were zero Cloud Logging entries and zero Dataflow jobs referencing this job at all beyond its initial creation. Training-side analysis clearly ran once (at creation); the periodic serving-side cycle never fired again after that. This reproduces the same pattern already documented for `avg_pick_time` on DirectRunner in project-04 (NOTES.md 2026-07-24) -- a platform/runner limitation that config and code can't work around, distinguished from a real bug by first proving every other piece (config correctness, logging pipeline, training baseline) works.

**Decision:**
Rather than keep an endpoint billing indefinitely chasing a scheduler that showed no sign of advancing, stopped after confirming config correctness + training baseline + live serving-log ingestion all work end-to-end -- three of the four pieces of the feature are independently verified live, with the periodic serving-side analysis documented as an unresolved platform-side gap rather than glossed over. Undeployed the model, paused (required before delete) and deleted the `ModelDeploymentMonitoringJob`, then deleted the endpoint. Confirmed via `GET` on both collections afterward that nothing remains.

**Gotchas/issues hit:**
A `ModelDeploymentMonitoringJob` cannot be deleted while `state: RUNNING` (`FAILED_PRECONDITION`) -- it must be paused first (`:pause`), which takes effect within a few seconds, before `DELETE` succeeds.
