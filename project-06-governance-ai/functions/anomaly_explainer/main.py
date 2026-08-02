"""Cloud Function: runs the project-06 Dataplex data quality scans, then
calls Gemini (via Vertex AI) to turn the raw rule results into a plain-
English governance report.

Self-contained on purpose: rather than reacting to a Dataplex/BigQuery
event (neither service exposes a "scan job completed" Eventarc event --
see NOTES.md), this function runs the scan, polls it to completion, reads
the results, and calls Gemini in one HTTP invocation. Intended to be
called by Cloud Scheduler (or manually, for verification).
"""

import logging
import time

import functions_framework

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
import google.auth
import google.auth.transport.requests
import requests
from google import genai

PROJECT_ID = "harsha-data-platform"
LOCATION = "us-central1"
DATAPLEX_BASE = (
    f"https://dataplex.googleapis.com/v1/projects/{PROJECT_ID}"
    f"/locations/{LOCATION}"
)

SCAN_IDS = [
    "fact-orders-quality",
    "dim-customers-quality",
    "dim-products-quality",
    "fact-daily-inventory-quality",
]

POLL_INTERVAL_SECONDS = 5
POLL_TIMEOUT_SECONDS = 180

# Reused across every Dataplex call in an invocation -- a bare requests.get
# / requests.post opens a fresh TCP+TLS connection each time, and under this
# function's low default CPU allocation that handshake overhead compounds
# badly across ~6 polling calls per scan x 4 scans (see NOTES.md).
_session = requests.Session()


def _access_token() -> str:
    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    credentials.refresh(google.auth.transport.requests.Request())
    return credentials.token


def _run_scan(token: str, scan_id: str) -> str:
    resp = _session.post(
        f"{DATAPLEX_BASE}/dataScans/{scan_id}:run",
        headers={"Authorization": f"Bearer {token}"},
        json={},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["job"]["uid"]


def _wait_for_job(token: str, scan_id: str, job_id: str) -> dict:
    deadline = time.time() + POLL_TIMEOUT_SECONDS
    while time.time() < deadline:
        resp = _session.get(
            f"{DATAPLEX_BASE}/dataScans/{scan_id}/jobs/{job_id}?view=FULL",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        resp.raise_for_status()
        job = resp.json()
        if job.get("state") in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return job
        time.sleep(POLL_INTERVAL_SECONDS)
    raise TimeoutError(f"{scan_id} job {job_id} did not finish within {POLL_TIMEOUT_SECONDS}s")


def _summarize_job(scan_id: str, job: dict) -> dict:
    result = job.get("dataQualityResult", {})
    row_count = result.get("rowCount", 0)
    rules = []
    for rule_result in result.get("rules", []):
        rule = rule_result.get("rule", {})
        evaluated = int(rule_result.get("evaluatedCount", 0))
        passed_count = int(rule_result.get("passedCount", 0))
        rules.append(
            {
                "column": rule.get("column"),
                "dimension": rule.get("dimension"),
                "expectation": next(
                    (
                        v.get("sqlExpression", key)
                        for key, v in rule.items()
                        if key.endswith("Expectation")
                    ),
                    "unknown",
                ),
                "passed": bool(rule_result.get("passed", False)),
                "evaluated_count": evaluated,
                "failing_count": evaluated - passed_count,
            }
        )
    return {
        "scan_id": scan_id,
        "table": scan_id.replace("-quality", "").replace("-", "_"),
        "row_count": row_count,
        "overall_passed": bool(result.get("passed", False)),
        "rules": rules,
    }


def _build_prompt(summaries: list[dict]) -> str:
    lines = [
        "You are a data governance analyst for PeakCart, a grocery delivery "
        "company's BigQuery data warehouse. Below are real Dataplex data "
        "quality scan results for the gold (production reporting) layer. "
        "Write a concise, plain-English report for a non-technical "
        "stakeholder: summarize what was checked, call out every failed "
        "rule by name with its actual failing row count and percentage, "
        "and briefly explain the likely real-world impact of each failure "
        "(e.g. what it means for reporting/analytics if the row was not "
        "caught). If a table passed every rule, say so plainly. Do not "
        "invent findings beyond what's given below.",
        "",
    ]
    for s in summaries:
        lines.append(f"Table: {s['table']} ({s['row_count']} rows scanned)")
        for r in s["rules"]:
            status = "PASSED" if r["passed"] else "FAILED"
            pct = (
                100.0 * r["failing_count"] / r["evaluated_count"]
                if r["evaluated_count"]
                else 0.0
            )
            lines.append(
                f"  - [{status}] {r['dimension']} check on column "
                f"'{r['column']}' ({r['expectation']}): "
                f"{r['failing_count']} of {r['evaluated_count']} rows failed "
                f"({pct:.2f}%)"
            )
        lines.append("")
    return "\n".join(lines)


@functions_framework.http
def run_governance_report(request):
    logger.info("run_governance_report: starting")
    token = _access_token()
    logger.info("run_governance_report: got access token")

    summaries = []
    for scan_id in SCAN_IDS:
        logger.info("run_governance_report: running scan %s", scan_id)
        job_id = _run_scan(token, scan_id)
        logger.info("run_governance_report: %s job_id=%s, polling", scan_id, job_id)
        job = _wait_for_job(token, scan_id, job_id)
        logger.info("run_governance_report: %s finished state=%s", scan_id, job.get("state"))
        summaries.append(_summarize_job(scan_id, job))

    prompt = _build_prompt(summaries)
    logger.info("run_governance_report: calling Gemini")

    client = genai.Client(vertexai=True, project=PROJECT_ID, location=LOCATION)
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
    )
    logger.info("run_governance_report: got Gemini response")

    return {
        "scan_summaries": summaries,
        "report": response.text,
    }
