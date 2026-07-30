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
    """Creates a Vertex AI endpoint and deploys vertex_model_id to it with
    disableExplanations=true. Calls the REST API directly because neither
    `gcloud ai endpoints deploy-model` nor ModelDeployOp expose a way to
    disable the auto-attached Shapley explanation spec that BQML gives
    Vertex-AI-registered models -- deploying with it enabled fails with a
    TensorFlow graph-version mismatch (see NOTES.md, Phase 3 entry)."""
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

    create_resp = requests.post(
        f"{base}/endpoints", headers=headers, json={"displayName": endpoint_display_name}
    )
    create_resp.raise_for_status()
    endpoint = wait_for_operation(create_resp.json()["name"], poll_seconds=10)
    endpoint_name = endpoint["name"]
    endpoint_id = endpoint_name.split("/")[-1]

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
        }
    }
    deploy_resp = requests.post(
        f"{base}/endpoints/{endpoint_id}:deployModel", headers=headers, json=deploy_body
    )
    deploy_resp.raise_for_status()
    wait_for_operation(deploy_resp.json()["name"], poll_seconds=20)

    print(f"Deployed {vertex_model_id} to endpoint {endpoint_name}")
    return endpoint_name


@dsl.pipeline(
    name="stockout-risk-training-pipeline",
    description="Trains the stockout risk model, evaluates it on held-out future data, and deploys it only if it clears a quality bar.",
)
def stockout_risk_pipeline(
    roc_auc_threshold: float = 0.8,
    endpoint_display_name: str = "stockout-risk-endpoint-pipeline",
):
    train_task = BigqueryCreateModelJobOp(
        query=TRAIN_QUERY,
        location=LOCATION,
        project=PROJECT_ID,
    )
    train_task.set_display_name("train-stockout-risk-model")

    evaluate_task = evaluate_model(
        project=PROJECT_ID,
        location=LOCATION,
        dataset=DATASET,
        model_name=MODEL_NAME,
    )
    evaluate_task.after(train_task)
    evaluate_task.set_display_name("evaluate-on-held-out-period")

    with dsl.If(evaluate_task.output >= roc_auc_threshold, name="deploy-if-good-enough"):
        deploy_task = deploy_model_without_explanations(
            project_number=PROJECT_NUMBER,
            location=LOCATION,
            vertex_model_id=VERTEX_MODEL_ID,
            endpoint_display_name=endpoint_display_name,
        )
        deploy_task.set_display_name("deploy-to-endpoint")


if __name__ == "__main__":
    compiler.Compiler().compile(
        pipeline_func=stockout_risk_pipeline,
        package_path="stockout_risk_pipeline.json",
    )
    print("Compiled to stockout_risk_pipeline.json")
