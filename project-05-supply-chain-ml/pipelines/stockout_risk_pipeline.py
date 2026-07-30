"""
Vertex AI Pipeline: train -> evaluate -> conditionally deploy the
stockout risk model.

This is the MLOps piece the manual Dataform/BQML/Vertex AI steps
(NOTES.md 2026-07-29 entries) didn't provide: a single automated job that
retrains the model, checks whether it's actually good enough, and only
deploys it if it clears a quality bar -- rather than three separate
manual steps with no gate between them.

Component choices, and why:
- Training uses google_cloud_pipeline_components' BigqueryCreateModelJobOp
  directly -- it's Google's own mature component for exactly this
  (handles the async BigQuery job + gcp_resources tracking correctly),
  no reason to reinvent it.
- Evaluation is a custom Python component using the BigQuery client
  directly, rather than BigqueryEvaluateModelJobOp. That component's
  evaluation_metrics output is an opaque system.Artifact; a custom
  component keeps the roc_auc extraction transparent and lets it be a
  first-class pipeline value the dsl.If condition can compare against.
- Deployment is a custom Python component calling the Vertex AI REST API
  directly, NOT google_cloud_pipeline_components' ModelDeployOp.
  ModelDeployOp has no disable_explanations-equivalent input (confirmed
  by inspecting its component_spec), so it would hit the same
  Explanation-preprocessing TensorFlow graph-version mismatch that broke
  the first manual deployment attempt in Phase 3. This component reuses
  that proven fix (disableExplanations=true via a raw REST call).
"""

from kfp import compiler, dsl
from kfp.dsl import component
from google_cloud_pipeline_components.v1.bigquery import BigqueryCreateModelJobOp

PROJECT_ID = "harsha-data-platform"
PROJECT_NUMBER = "435348575003"
LOCATION = "us-central1"
DATASET = "peakcart_supply_chain_features"
MODEL_NAME = "stockout_risk_model"
VERTEX_MODEL_ID = "stockout-risk-model"

TRAIN_QUERY = f"""
CREATE OR REPLACE MODEL `{PROJECT_ID}.{DATASET}.{MODEL_NAME}`
OPTIONS (
  model_type = 'LOGISTIC_REG',
  input_label_cols = ['stockout_next_7d'],
  auto_class_weights = true,
  model_registry = 'vertex_ai',
  vertex_ai_model_id = '{VERTEX_MODEL_ID}',
  vertex_ai_model_version_aliases = ['pipeline']
) AS
SELECT
  qty_on_hand,
  rolling_7d_avg_demand,
  lead_time_days,
  category,
  price,
  stockout_next_7d
FROM `{PROJECT_ID}.{DATASET}.stockout_risk_features`
WHERE date < '2025-11-01'
"""


@component(
    base_image="python:3.11",
    packages_to_install=["google-auth==2.35.0", "requests==2.32.3"],
)
def undeploy_existing_model(
    project_number: str,
    location: str,
    endpoint_display_name: str,
) -> str:
    """Undeploys whatever model is currently serving on endpoint_display_name
    (if the endpoint exists at all), WITHOUT deleting the endpoint itself.

    Must run before training: BQML refuses `CREATE OR REPLACE MODEL` while
    that model is actively deployed to a Vertex AI endpoint --
    `FAILED_PRECONDITION: The Model is deployed or being deployed at the
    following Endpoint(s)... Please undeploy the model before retry.`
    Found this the hard way on a live scheduled run: training failed
    outright the second time the pipeline ran against an
    already-successfully-deployed model. This does mean the endpoint has
    no serving model for the duration of the retrain -- a real trade-off,
    not hidden here. A zero-downtime version would train a new model
    version under a different Vertex AI model resource and blue-green
    swap it in, which is real added complexity this project didn't need
    to take on to demonstrate the core gate (train -> evaluate -> deploy
    only if good enough)."""
    import google.auth
    import google.auth.transport.requests
    import requests

    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    creds.refresh(google.auth.transport.requests.Request())
    headers = {"Authorization": f"Bearer {creds.token}", "Content-Type": "application/json"}

    api_root = f"https://{location}-aiplatform.googleapis.com/v1"
    base = f"{api_root}/projects/{project_number}/locations/{location}"

    list_resp = requests.get(
        f"{base}/endpoints",
        headers=headers,
        params={"filter": f'display_name="{endpoint_display_name}"'},
    )
    list_resp.raise_for_status()
    existing_endpoints = list_resp.json().get("endpoints", [])

    if not existing_endpoints:
        print(f"No endpoint named {endpoint_display_name} yet -- nothing to undeploy.")
        return "no-endpoint"

    endpoint = existing_endpoints[0]
    endpoint_id = endpoint["name"].split("/")[-1]
    deployed_model_ids = [dm["id"] for dm in endpoint.get("deployedModels", [])]

    if not deployed_model_ids:
        print(f"Endpoint {endpoint['name']} exists but has nothing deployed.")
        return "nothing-deployed"

    for deployed_id in deployed_model_ids:
        resp = requests.post(
            f"{base}/endpoints/{endpoint_id}:undeployModel",
            headers=headers,
            json={"deployedModelId": deployed_id},
        )
        resp.raise_for_status()
        op_name = resp.json()["name"]
        while True:
            import time

            r = requests.get(f"{api_root}/{op_name}", headers=headers)
            r.raise_for_status()
            body = r.json()
            if body.get("done"):
                if "error" in body:
                    raise RuntimeError(f"Undeploy failed: {body['error']}")
                break
            time.sleep(10)
        print(f"Undeployed {deployed_id} from {endpoint['name']}")

    return "undeployed"


@component(
    base_image="python:3.11",
    packages_to_install=["google-cloud-bigquery==3.42.2"],
)
def evaluate_model(project: str, location: str, dataset: str, model_name: str) -> float:
    """Runs ML.EVALUATE on the held-out Nov-Dec period (never trained on)
    and returns roc_auc as a plain float the pipeline can branch on."""
    from google.cloud import bigquery

    client = bigquery.Client(project=project, location=location)
    query = f"""
    SELECT roc_auc
    FROM ML.EVALUATE(MODEL `{project}.{dataset}.{model_name}`,
      (SELECT qty_on_hand, rolling_7d_avg_demand, lead_time_days, category, price, stockout_next_7d
       FROM `{project}.{dataset}.stockout_risk_features`
       WHERE date >= '2025-11-01'))
    """
    row = list(client.query(query).result())[0]
    roc_auc = float(row.roc_auc)
    print(f"ROC AUC on held-out Nov-Dec period: {roc_auc}")
    return roc_auc


@component(
    base_image="python:3.11",
    packages_to_install=["google-auth==2.35.0", "requests==2.32.3"],
)
def deploy_model_without_explanations(
    project_number: str,
    location: str,
    vertex_model_id: str,
    endpoint_display_name: str,
) -> str:
    """Deploys vertex_model_id to a stable, reused endpoint (identified by
    endpoint_display_name) with disableExplanations=true, and undeploys
    whatever model version was serving there before.

    Calls the REST API directly because neither `gcloud ai endpoints
    deploy-model` nor ModelDeployOp expose a way to disable the
    auto-attached Shapley explanation spec that BQML gives Vertex-AI-
    registered models -- deploying with it enabled fails with a
    TensorFlow graph-version mismatch (see NOTES.md, Phase 3 entry).

    Reuses an existing endpoint with this display name instead of always
    creating a new one -- necessary for scheduled/recurring runs, since
    otherwise every successful retrain would spin up another live,
    billing endpoint that accumulates indefinitely. Uses the documented
    "0" placeholder key in trafficSplit to mean "the model being deployed
    by this call" (its real deployed-model ID doesn't exist yet at
    request time), giving it 100% traffic while dropping any existing
    deployed model to 0%, then explicitly undeploys that old one so it
    stops serving (and billing) entirely rather than sitting idle."""
    import time

    import google.auth
    import google.auth.transport.requests
    import requests

    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    creds.refresh(google.auth.transport.requests.Request())
    headers = {"Authorization": f"Bearer {creds.token}", "Content-Type": "application/json"}

    api_root = f"https://{location}-aiplatform.googleapis.com/v1"
    base = f"{api_root}/projects/{project_number}/locations/{location}"

    def wait_for_operation(operation_name, poll_seconds):
        while True:
            r = requests.get(f"{api_root}/{operation_name}", headers=headers)
            r.raise_for_status()
            body = r.json()
            if body.get("done"):
                if "error" in body:
                    raise RuntimeError(f"Operation {operation_name} failed: {body['error']}")
                return body["response"]
            time.sleep(poll_seconds)

    list_resp = requests.get(
        f"{base}/endpoints",
        headers=headers,
        params={"filter": f'display_name="{endpoint_display_name}"'},
    )
    list_resp.raise_for_status()
    existing_endpoints = list_resp.json().get("endpoints", [])

    if existing_endpoints:
        endpoint = existing_endpoints[0]
        endpoint_name = endpoint["name"]
        endpoint_id = endpoint_name.split("/")[-1]
        old_deployed_model_ids = [dm["id"] for dm in endpoint.get("deployedModels", [])]
        print(f"Reusing existing endpoint {endpoint_name} (old deployed models: {old_deployed_model_ids})")
    else:
        create_resp = requests.post(
            f"{base}/endpoints", headers=headers, json={"displayName": endpoint_display_name}
        )
        create_resp.raise_for_status()
        endpoint = wait_for_operation(create_resp.json()["name"], poll_seconds=10)
        endpoint_name = endpoint["name"]
        endpoint_id = endpoint_name.split("/")[-1]
        old_deployed_model_ids = []
        print(f"Created new endpoint {endpoint_name}")

    traffic_split = {"0": 100}
    for old_id in old_deployed_model_ids:
        traffic_split[old_id] = 0

    deploy_body = {
        "deployedModel": {
            "model": f"projects/{project_number}/locations/{location}/models/{vertex_model_id}",
            "displayName": f"{vertex_model_id}-deployment",
            "dedicatedResources": {
                "machineSpec": {"machineType": "n1-standard-2"},
                "minReplicaCount": 1,
                "maxReplicaCount": 1,
            },
            "disableExplanations": True,
        },
        "trafficSplit": traffic_split,
    }
    deploy_resp = requests.post(
        f"{base}/endpoints/{endpoint_id}:deployModel", headers=headers, json=deploy_body
    )
    deploy_resp.raise_for_status()
    wait_for_operation(deploy_resp.json()["name"], poll_seconds=20)
    print(f"Deployed new version of {vertex_model_id} to endpoint {endpoint_name}, traffic shifted to it")

    for old_id in old_deployed_model_ids:
        undeploy_resp = requests.post(
            f"{base}/endpoints/{endpoint_id}:undeployModel",
            headers=headers,
            json={"deployedModelId": old_id},
        )
        undeploy_resp.raise_for_status()
        wait_for_operation(undeploy_resp.json()["name"], poll_seconds=10)
        print(f"Undeployed old model version {old_id}")

    return endpoint_name


@dsl.pipeline(
    name="stockout-risk-training-pipeline",
    description="Trains the stockout risk model, evaluates it on held-out future data, and deploys it only if it clears a quality bar.",
)
def stockout_risk_pipeline(
    roc_auc_threshold: float = 0.8,
    endpoint_display_name: str = "stockout-risk-endpoint-pipeline",
):
    # Caching is disabled on every task here deliberately: Vertex AI
    # Pipelines caches a task's execution whenever its inputs match a
    # prior run, REGARDLESS of side effects. With fixed parameter
    # defaults (the normal case for a recurring schedule), every
    # subsequent run's inputs are identical to the first, so the default
    # (enabled) caching silently no-ops the entire pipeline forever --
    # confirmed live: a second run reused the first run's cached
    # deploy-model-without-explanations output unchanged, meaning nothing
    # was actually retrained, evaluated, or redeployed. Training and
    # evaluation need to rerun to reflect new data; deployment is a real
    # side effect (swapping live traffic) that must never be silently
    # skipped.
    undeploy_task = undeploy_existing_model(
        project_number=PROJECT_NUMBER,
        location=LOCATION,
        endpoint_display_name=endpoint_display_name,
    )
    undeploy_task.set_display_name("undeploy-existing-model-before-retrain")
    undeploy_task.set_caching_options(False)

    train_task = BigqueryCreateModelJobOp(
        query=TRAIN_QUERY,
        location=LOCATION,
        project=PROJECT_ID,
    )
    train_task.after(undeploy_task)
    train_task.set_display_name("train-stockout-risk-model")
    train_task.set_caching_options(False)

    evaluate_task = evaluate_model(
        project=PROJECT_ID,
        location=LOCATION,
        dataset=DATASET,
        model_name=MODEL_NAME,
    )
    evaluate_task.after(train_task)
    evaluate_task.set_display_name("evaluate-on-held-out-period")
    evaluate_task.set_caching_options(False)

    with dsl.If(evaluate_task.output >= roc_auc_threshold, name="deploy-if-good-enough"):
        deploy_task = deploy_model_without_explanations(
            project_number=PROJECT_NUMBER,
            location=LOCATION,
            vertex_model_id=VERTEX_MODEL_ID,
            endpoint_display_name=endpoint_display_name,
        )
        deploy_task.set_display_name("deploy-to-endpoint")
        deploy_task.set_caching_options(False)


if __name__ == "__main__":
    compiler.Compiler().compile(
        pipeline_func=stockout_risk_pipeline,
        package_path="stockout_risk_pipeline.json",
    )
    print("Compiled to stockout_risk_pipeline.json")
