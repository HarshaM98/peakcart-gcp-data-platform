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
