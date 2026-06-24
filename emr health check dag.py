"""
EMR Health Check DAG
=====================
Cluster  : emr-mars-03-prod  (ap-southeast-2)
Schedule : Daily at 06:00 AM UTC

What this DAG does
------------------
1. Finds the EMR cluster by Name tag — no hardcoded cluster ID
   since Jenkins destroys and recreates the cluster every day.
2. Reads min/max node thresholds DYNAMICALLY from list_instance_groups
   via AutoScalingPolicy.MinCapacity / MaxCapacity.
   Primary node falls back to RequestedInstanceCount (no auto-scaling).
3. Counts only RUNNING instances per group — BOOTSTRAPPING,
   TERMINATING and PENDING nodes are completely ignored.
4. Decides HEALTHY or UNHEALTHY:
     HEALTHY   →  all groups: running >= min
     UNHEALTHY →  cluster missing OR any group: running < min
5. Sends notifications:
     HEALTHY   →  SES email only
     UNHEALTHY →  SES email + SNS SMS

Retry Policy
------------
retries = 0 on every task — fail fast, alert immediately.

Airflow Variables Required
--------------------------
EMR_CLUSTER_NAME      emr-mars-03-prod
AWS_REGION            ap-southeast-2
SES_SENDER_EMAIL      emr-alerts@yourcompany.com
SES_RECIPIENT_EMAILS  etl-team@yourcompany.com   (comma-separated)
SNS_TOPIC_ARN         arn:aws:sns:ap-southeast-2:ACCOUNT_ID:emr-oncall-sms

Author  : ETL Support Engineering
Version : 2.0.0
"""

# ─────────────────────────────────────────────────────────────────
# Imports
# ─────────────────────────────────────────────────────────────────
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────
# Configuration — read from Airflow Variables
# Only the cluster NAME is needed — everything else is dynamic
# ─────────────────────────────────────────────────────────────────
CLUSTER_NAME     = Variable.get("EMR_CLUSTER_NAME",     default_var="emr-mars-03-prod")
AWS_REGION       = Variable.get("AWS_REGION",           default_var="ap-southeast-2")
SES_SENDER       = Variable.get("SES_SENDER_EMAIL",     default_var="emr-alerts@yourcompany.com")
SES_RECIPIENTS   = Variable.get("SES_RECIPIENT_EMAILS", default_var="etl-team@yourcompany.com").split(",")
SNS_TOPIC_ARN    = Variable.get("SNS_TOPIC_ARN",        default_var="arn:aws:sns:ap-southeast-2:123456789012:emr-oncall-sms")

# ─────────────────────────────────────────────────────────────────
# XCom keys — consistent naming across all tasks
# ─────────────────────────────────────────────────────────────────
XCOM_CLUSTER_ID       = "cluster_id"
XCOM_CLUSTER_STATE    = "cluster_state"
XCOM_CLUSTER_CREATED  = "cluster_created_at"
XCOM_PRIMARY_RESULT   = "primary_node_result"
XCOM_CORE_RESULT      = "core_node_result"
XCOM_TASK_RESULT      = "task_node_result"
XCOM_OVERALL_HEALTH   = "overall_health"

# Branch task IDs
BRANCH_HEALTHY    = "send_healthy_email"
BRANCH_UNHEALTHY  = "send_unhealthy_email"

# Instance group type identifiers as used by AWS EMR API
IGT_PRIMARY = "MASTER"
IGT_CORE    = "CORE"
IGT_TASK    = "TASK"


# ─────────────────────────────────────────────────────────────────
# AWS client helpers
# ─────────────────────────────────────────────────────────────────
def get_emr_client():
    return boto3.client("emr", region_name=AWS_REGION)

def get_ses_client():
    return boto3.client("ses", region_name=AWS_REGION)

def get_sns_client():
    return boto3.client("sns", region_name=AWS_REGION)


# ─────────────────────────────────────────────────────────────────
# Core helpers
# ─────────────────────────────────────────────────────────────────

def find_cluster_by_name(cluster_name: str) -> dict | None:
    """
    Paginate list_clusters (ACTIVE states only) and match by Name tag.
    Returns the full describe_cluster response dict or None.

    We search by Name tag because the cluster ID changes every day
    when Jenkins destroys and recreates the cluster.
    Active states searched: STARTING, BOOTSTRAPPING, RUNNING, WAITING.
    """
    client = get_emr_client()
    active_states = ["STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING"]
    paginator = client.get_paginator("list_clusters")

    for page in paginator.paginate(ClusterStates=active_states):
        for cluster_summary in page.get("Clusters", []):
            cluster_id = cluster_summary["Id"]
            detail = client.describe_cluster(ClusterId=cluster_id)["Cluster"]
            tag_map = {t["Key"]: t["Value"] for t in detail.get("Tags", [])}
            if tag_map.get("Name") == cluster_name:
                log.info("Matched cluster: %s → %s", cluster_name, cluster_id)
                return detail

    return None


def get_instance_groups(cluster_id: str) -> list[dict]:
    """
    Call list_instance_groups and return all groups for the cluster.
    Each group dict contains:
        InstanceGroupType        MASTER / CORE / TASK
        RunningInstanceCount     currently running instances
        RequestedInstanceCount   instances requested at launch
        AutoScalingPolicy        contains MinCapacity / MaxCapacity
                                 (present on CORE and TASK, absent on MASTER)
    """
    client = get_emr_client()
    response = client.list_instance_groups(ClusterId=cluster_id)
    return response.get("InstanceGroups", [])


def extract_min_max(group: dict) -> tuple[int, int]:
    """
    Extract min and max node counts from a single instance group.

    For CORE and TASK groups:
        Reads AutoScalingPolicy.Constraints.MinCapacity / MaxCapacity
        which are set via the Custom Automatic Scaling policy in the
        EMR console (as seen in the cluster screenshot).

    For PRIMARY (MASTER) group:
        No auto-scaling policy exists — Primary is always a fixed count.
        We use RequestedInstanceCount for both min and max.
    """
    policy = group.get("AutoScalingPolicy", {})
    constraints = policy.get("Constraints", {})

    if constraints:
        min_cap = constraints.get("MinCapacity", 1)
        max_cap = constraints.get("MaxCapacity", 1)
    else:
        # Primary node — fixed count, no auto-scaling
        requested = group.get("RequestedInstanceCount", 1)
        min_cap = requested
        max_cap = requested

    return int(min_cap), int(max_cap)


def count_instances_by_state(cluster_id: str, instance_group_type: str) -> dict:
    """
    Use list_instances to count instances per state for a given group type.
    Only RUNNING count is used for the health decision.
    Other states are counted for display in the email only.
    """
    client = get_emr_client()
    paginator = client.get_paginator("list_instances")

    counts = {
        "RUNNING":      0,
        "BOOTSTRAPPING":0,
        "PROVISIONING": 0,
        "TERMINATING":  0,
        "TERMINATED":   0,
    }

    for page in paginator.paginate(
        ClusterId=cluster_id,
        InstanceGroupTypes=[instance_group_type],
    ):
        for instance in page.get("Instances", []):
            state = instance.get("Status", {}).get("State", "UNKNOWN")
            if state in counts:
                counts[state] += 1

    return counts


def build_node_result(
    group_type:    str,
    display_name:  str,
    symbol:        str,
    running:       int,
    bootstrapping: int,
    terminating:   int,
    min_nodes:     int,
    max_nodes:     int,
) -> dict:
    """
    Build a standardised node result dict that is pushed to XCom
    and consumed by evaluate_overall_health and both notification tasks.
    """
    verdict = "PASS" if running >= min_nodes else "FAIL"
    deficit = max(0, min_nodes - running)

    if verdict == "PASS":
        note = f"Running ({running}) >= min ({min_nodes})"
        if bootstrapping > 0:
            note += f" | {bootstrapping} bootstrapping (not counted)"
    else:
        note = f"Running ({running}) < min ({min_nodes}) — DEFICIT of {deficit}"
        if bootstrapping > 0:
            note += f" | {bootstrapping} bootstrapping (not counted)"

    return {
        "group_type":    group_type,
        "display_name":  display_name,
        "symbol":        symbol,
        "running":       running,
        "bootstrapping": bootstrapping,
        "terminating":   terminating,
        "min":           min_nodes,
        "max":           max_nodes,
        "verdict":       verdict,
        "deficit":       deficit,
        "note":          note,
    }


def missing_cluster_sentinel(xcom_key: str) -> str:
    """
    Build a FAIL sentinel node result for when the cluster is not found.
    Pushed to XCom by check_cluster_exists so evaluate_overall_health
    can aggregate correctly without node check tasks running.
    """
    name_map = {
        XCOM_PRIMARY_RESULT: ("Primary", "P"),
        XCOM_CORE_RESULT:    ("Core",    "C"),
        XCOM_TASK_RESULT:    ("Task",    "T"),
    }
    display, symbol = name_map.get(xcom_key, ("Unknown", "?"))
    return json.dumps({
        "group_type":    xcom_key,
        "display_name":  display,
        "symbol":        symbol,
        "running":       0,
        "bootstrapping": 0,
        "terminating":   0,
        "min":           0,
        "max":           0,
        "verdict":       "FAIL",
        "deficit":       0,
        "note":          "Cluster not found — cannot check nodes",
    })


# ─────────────────────────────────────────────────────────────────
# Task 1 — check_cluster_exists
# ─────────────────────────────────────────────────────────────────
def check_cluster_exists(**context) -> None:
    """
    Find the EMR cluster by Name tag.

    If FOUND  → push cluster_id, cluster_state, cluster_created_at to XCom.
    If NOT FOUND → push NOT_FOUND sentinels for cluster info AND
                   FAIL sentinels for all 3 node groups so that
                   evaluate_overall_health can still run without
                   the node check tasks doing anything.
    """
    ti = context["ti"]
    log.info("Searching for EMR cluster: '%s' in region: %s", CLUSTER_NAME, AWS_REGION)

    cluster = find_cluster_by_name(CLUSTER_NAME)

    if cluster is None:
        log.warning(
            "Cluster '%s' NOT FOUND. Jenkins pipeline may not have run today.",
            CLUSTER_NAME,
        )
        ti.xcom_push(key=XCOM_CLUSTER_ID,      value="NOT_FOUND")
        ti.xcom_push(key=XCOM_CLUSTER_STATE,   value="NOT_FOUND")
        ti.xcom_push(key=XCOM_CLUSTER_CREATED, value="N/A")
        # Push FAIL sentinels for all node groups
        for xcom_key in [XCOM_PRIMARY_RESULT, XCOM_CORE_RESULT, XCOM_TASK_RESULT]:
            ti.xcom_push(key=xcom_key, value=missing_cluster_sentinel(xcom_key))
        return

    cluster_id    = cluster["Id"]
    cluster_state = cluster["Status"]["State"]
    created_dt    = cluster.get("Status", {}).get("Timeline", {}).get("CreationDateTime")
    created_str   = (
        created_dt.strftime("%d %b %Y, %I:%M %p %Z")
        if created_dt else "Unknown"
    )

    log.info(
        "Cluster found | ID: %s | State: %s | Created: %s",
        cluster_id, cluster_state, created_str,
    )

    ti.xcom_push(key=XCOM_CLUSTER_ID,      value=cluster_id)
    ti.xcom_push(key=XCOM_CLUSTER_STATE,   value=cluster_state)
    ti.xcom_push(key=XCOM_CLUSTER_CREATED, value=created_str)


# ─────────────────────────────────────────────────────────────────
# Task 2 — check_primary_nodes
# ─────────────────────────────────────────────────────────────────
def check_primary_nodes(**context) -> None:
    """
    Check Primary (MASTER) node group.
    Min/max read from RequestedInstanceCount (no auto-scaling on Primary).
    Only RUNNING instances count toward the minimum.
    """
    ti         = context["ti"]
    cluster_id = ti.xcom_pull(key=XCOM_CLUSTER_ID, task_ids="check_cluster_exists")

    if cluster_id == "NOT_FOUND":
        log.info("Cluster not found — skipping primary node check.")
        return

    log.info("Checking PRIMARY nodes | Cluster: %s", cluster_id)

    # Get the MASTER instance group to read min/max dynamically
    all_groups = get_instance_groups(cluster_id)
    primary_group = next(
        (g for g in all_groups if g["InstanceGroupType"] == IGT_PRIMARY),
        None,
    )

    if primary_group is None:
        log.warning("No PRIMARY instance group found for cluster: %s", cluster_id)
        ti.xcom_push(key=XCOM_PRIMARY_RESULT, value=json.dumps(
            build_node_result("MASTER", "Primary", "P", 0, 0, 0, 1, 1)
        ))
        return

    min_nodes, max_nodes = extract_min_max(primary_group)
    counts = count_instances_by_state(cluster_id, IGT_PRIMARY)

    result = build_node_result(
        group_type   = IGT_PRIMARY,
        display_name = "Primary",
        symbol       = "P",
        running      = counts["RUNNING"],
        bootstrapping= counts["BOOTSTRAPPING"],
        terminating  = counts["TERMINATING"],
        min_nodes    = min_nodes,
        max_nodes    = max_nodes,
    )
    log.info("Primary result: %s", result)
    ti.xcom_push(key=XCOM_PRIMARY_RESULT, value=json.dumps(result))


# ─────────────────────────────────────────────────────────────────
# Task 3 — check_core_nodes
# ─────────────────────────────────────────────────────────────────
def check_core_nodes(**context) -> None:
    """
    Check Core node group.
    Min/max read from AutoScalingPolicy.Constraints (MinCapacity/MaxCapacity).
    Only RUNNING instances count toward the minimum.
    """
    ti         = context["ti"]
    cluster_id = ti.xcom_pull(key=XCOM_CLUSTER_ID, task_ids="check_cluster_exists")

    if cluster_id == "NOT_FOUND":
        log.info("Cluster not found — skipping core node check.")
        return

    log.info("Checking CORE nodes | Cluster: %s", cluster_id)

    all_groups = get_instance_groups(cluster_id)
    core_group = next(
        (g for g in all_groups if g["InstanceGroupType"] == IGT_CORE),
        None,
    )

    if core_group is None:
        log.warning("No CORE instance group found for cluster: %s", cluster_id)
        ti.xcom_push(key=XCOM_CORE_RESULT, value=json.dumps(
            build_node_result("CORE", "Core", "C", 0, 0, 0, 1, 1)
        ))
        return

    min_nodes, max_nodes = extract_min_max(core_group)
    counts = count_instances_by_state(cluster_id, IGT_CORE)

    result = build_node_result(
        group_type   = IGT_CORE,
        display_name = "Core",
        symbol       = "C",
        running      = counts["RUNNING"],
        bootstrapping= counts["BOOTSTRAPPING"],
        terminating  = counts["TERMINATING"],
        min_nodes    = min_nodes,
        max_nodes    = max_nodes,
    )
    log.info("Core result: %s", result)
    ti.xcom_push(key=XCOM_CORE_RESULT, value=json.dumps(result))


# ─────────────────────────────────────────────────────────────────
# Task 4 — check_task_nodes
# ─────────────────────────────────────────────────────────────────
def check_task_nodes(**context) -> None:
    """
    Check Task node group.
    Min/max read from AutoScalingPolicy.Constraints (MinCapacity/MaxCapacity).
    Only RUNNING instances count toward the minimum.
    """
    ti         = context["ti"]
    cluster_id = ti.xcom_pull(key=XCOM_CLUSTER_ID, task_ids="check_cluster_exists")

    if cluster_id == "NOT_FOUND":
        log.info("Cluster not found — skipping task node check.")
        return

    log.info("Checking TASK nodes | Cluster: %s", cluster_id)

    all_groups = get_instance_groups(cluster_id)
    task_group = next(
        (g for g in all_groups if g["InstanceGroupType"] == IGT_TASK),
        None,
    )

    if task_group is None:
        log.warning("No TASK instance group found for cluster: %s", cluster_id)
        ti.xcom_push(key=XCOM_TASK_RESULT, value=json.dumps(
            build_node_result("TASK", "Task", "T", 0, 0, 0, 1, 1)
        ))
        return

    min_nodes, max_nodes = extract_min_max(task_group)
    counts = count_instances_by_state(cluster_id, IGT_TASK)

    result = build_node_result(
        group_type   = IGT_TASK,
        display_name = "Task",
        symbol       = "T",
        running      = counts["RUNNING"],
        bootstrapping= counts["BOOTSTRAPPING"],
        terminating  = counts["TERMINATING"],
        min_nodes    = min_nodes,
        max_nodes    = max_nodes,
    )
    log.info("Task result: %s", result)
    ti.xcom_push(key=XCOM_TASK_RESULT, value=json.dumps(result))


# ─────────────────────────────────────────────────────────────────
# Task 5 — evaluate_overall_health (BranchPythonOperator)
# ─────────────────────────────────────────────────────────────────
def evaluate_overall_health(**context) -> str:
    """
    Pull all XCom results and decide HEALTHY or UNHEALTHY.

    Returns the task_id of the next branch to run:
        HEALTHY   → "send_healthy_email"
        UNHEALTHY → "send_unhealthy_email"

    Cluster missing scenario:
        check_cluster_exists pushes FAIL sentinels for all node groups
        so this task always has data to work with regardless of
        whether node check tasks ran.
    """
    ti            = context["ti"]
    cluster_id    = ti.xcom_pull(key=XCOM_CLUSTER_ID,    task_ids="check_cluster_exists")
    cluster_state = ti.xcom_pull(key=XCOM_CLUSTER_STATE, task_ids="check_cluster_exists")

    is_cluster_missing = cluster_id == "NOT_FOUND"

    # Pull node results — if cluster was missing, node tasks were
    # skipped so we fall back to sentinels from check_cluster_exists
    def pull_node_result(xcom_key: str, node_task_id: str) -> dict:
        raw = ti.xcom_pull(key=xcom_key, task_ids=node_task_id)
        if not raw:
            raw = ti.xcom_pull(key=xcom_key, task_ids="check_cluster_exists")
        return json.loads(raw) if raw else {}

    primary = pull_node_result(XCOM_PRIMARY_RESULT, "check_primary_nodes")
    core    = pull_node_result(XCOM_CORE_RESULT,    "check_core_nodes")
    task    = pull_node_result(XCOM_TASK_RESULT,    "check_task_nodes")

    failed_groups = [
        n["display_name"]
        for n in [primary, core, task]
        if n.get("verdict") == "FAIL"
    ]

    is_unhealthy = is_cluster_missing or bool(failed_groups)
    overall      = "UNHEALTHY" if is_unhealthy else "HEALTHY"

    ti.xcom_push(key=XCOM_OVERALL_HEALTH, value=overall)

    log.info(
        "Health evaluation complete | Overall: %s | Cluster: %s | State: %s | Failed groups: %s",
        overall, cluster_id, cluster_state, failed_groups,
    )

    return BRANCH_UNHEALTHY if is_unhealthy else BRANCH_HEALTHY


# ─────────────────────────────────────────────────────────────────
# Email HTML builder
# Outlook-compatible rules applied:
#   - No <style> block — fully inline CSS only
#   - Table-based layout (not flexbox/grid) for Classic Outlook
#   - Arial font stack throughout
#   - No border-radius on outer containers (stripped by Outlook)
#   - No box-shadow
#   - MSO conditional comments in <head>
#   - HTML entities instead of raw emoji
# ─────────────────────────────────────────────────────────────────

def _verdict_color(verdict: str) -> str:
    return "#059669" if verdict == "PASS" else "#DC2626"

def _verdict_bg(verdict: str) -> str:
    return "#ECFDF5" if verdict == "PASS" else "#FEF2F2"


def _node_row_html(node: dict) -> str:
    vc       = _verdict_color(node["verdict"])
    vbg      = _verdict_bg(node["verdict"])
    run_w    = min(int((node["running"]  / max(node["max"], 1)) * 120), 120)
    boot_w   = min(int(((node["running"] + node["bootstrapping"]) / max(node["max"], 1)) * 120), 120)
    boot_txt = f'{node["bootstrapping"]} bootstrapping (ignored)' if node["bootstrapping"] > 0 else "&nbsp;"
    return f"""
      <tr>
        <td style="padding:12px 14px;border-bottom:1px solid #E2E8F0;
                   font-family:Arial,sans-serif;font-size:13px;
                   color:#0F172A;font-weight:700;width:80px;">
          {node["display_name"]}
        </td>
        <td style="padding:12px 14px;border-bottom:1px solid #E2E8F0;
                   font-family:Arial,sans-serif;font-size:22px;
                   font-weight:800;color:{vc};text-align:center;width:60px;">
          {node["running"]}
        </td>
        <td style="padding:12px 14px;border-bottom:1px solid #E2E8F0;
                   font-family:Arial,sans-serif;font-size:12px;
                   color:#64748B;text-align:center;width:80px;">
          {node["min"]} / {node["max"]}
        </td>
        <td style="padding:12px 14px;border-bottom:1px solid #E2E8F0;width:150px;">
          <!--[if mso]>
          <table cellpadding="0" cellspacing="0" border="0"
                 width="120" style="background-color:#E2E8F0;">
            <tr><td width="{boot_w}" height="8"
                    style="background-color:{vc}40;font-size:0;">&nbsp;</td>
                <td>&nbsp;</td></tr>
          </table>
          <table cellpadding="0" cellspacing="0" border="0"
                 width="120" style="margin-top:-6px;">
            <tr><td width="{run_w}" height="4"
                    style="background-color:{vc};font-size:0;">&nbsp;</td>
                <td>&nbsp;</td></tr>
          </table>
          <![endif]-->
          <!--[if !mso]><!-->
          <div style="width:120px;height:8px;background-color:#E2E8F0;
                      border-radius:4px;overflow:hidden;">
            <div style="width:{boot_w}px;height:8px;
                        background-color:{vc}40;border-radius:4px;"></div>
          </div>
          <div style="width:{run_w}px;height:4px;
                      background-color:{vc};border-radius:4px;
                      margin-top:-6px;"></div>
          <!--<![endif]-->
          <p style="margin:5px 0 0;font-family:Arial,sans-serif;
                    font-size:10px;color:#94A3B8;">{boot_txt}</p>
        </td>
        <td style="padding:12px 14px;border-bottom:1px solid #E2E8F0;
                   font-family:Arial,sans-serif;font-size:12px;color:{vc};">
          {node["note"]}
        </td>
        <td style="padding:12px 14px;border-bottom:1px solid #E2E8F0;
                   text-align:center;width:70px;">
          <span style="display:inline-block;padding:3px 10px;
                       border-radius:12px;font-family:Arial,sans-serif;
                       font-size:10px;font-weight:700;letter-spacing:0.5px;
                       color:{vc};background-color:{vbg};">
            {node["verdict"]}
          </span>
        </td>
      </tr>"""


def _check_row_html(label: str, result: str, detail: str) -> str:
    vc     = _verdict_color(result)
    vbg    = _verdict_bg(result)
    row_bg = "#FFF8F8" if result == "FAIL" else "#FFFFFF"
    detail_color = "#DC2626" if result == "FAIL" else "#64748B"
    return f"""
      <tr style="background-color:{row_bg};">
        <td style="padding:10px 14px;border-bottom:1px solid #E2E8F0;
                   font-family:Arial,sans-serif;font-size:13px;color:#0F172A;">
          {label}
        </td>
        <td style="padding:10px 14px;border-bottom:1px solid #E2E8F0;
                   text-align:center;width:70px;">
          <span style="display:inline-block;padding:2px 9px;
                       border-radius:12px;font-family:Arial,sans-serif;
                       font-size:10px;font-weight:700;letter-spacing:0.5px;
                       color:{vc};background-color:{vbg};">
            {result}
          </span>
        </td>
        <td style="padding:10px 14px;border-bottom:1px solid #E2E8F0;
                   font-family:Arial,sans-serif;font-size:12px;
                   color:{detail_color};">
          {detail}
        </td>
      </tr>"""


def build_email_html(
    status:          str,
    cluster_id:      str,
    cluster_state:   str,
    cluster_created: str,
    nodes:           list[dict],
    checks:          list[dict],
    dag_id:          str,
    run_id:          str,
    report_time:     str,
    sms_fired:       bool,
) -> str:
    is_healthy     = status == "HEALTHY"
    accent         = "#059669" if is_healthy else "#DC2626"
    accent_bg      = "#ECFDF5" if is_healthy else "#FEF2F2"
    accent_border  = "#A7F3D0" if is_healthy else "#FECACA"
    status_icon    = "&#10003;" if is_healthy else "&#10005;"

    node_rows  = "".join(_node_row_html(n) for n in nodes)
    check_rows = "".join(_check_row_html(c["label"], c["result"], c["detail"]) for c in checks)

    # SMS banner — only shown in UNHEALTHY email
    sms_banner = ""
    if sms_fired:
        sms_banner = """
        <tr>
          <td style="padding:10px 28px;background-color:#FEF2F2;
                     border-left:4px solid #DC2626;">
            <p style="margin:0;font-family:Arial,sans-serif;font-size:12px;
                      color:#991B1B;font-weight:700;">
              &#128241;&nbsp;SMS alert dispatched to on-call engineers via AWS SNS.
            </p>
          </td>
        </tr>"""

    # Cluster missing banner — shown when Jenkins failed
    cluster_missing_banner = ""
    if cluster_id == "NOT_FOUND":
        cluster_missing_banner = """
        <tr>
          <td style="padding:10px 28px;background-color:#FFFBEB;
                     border-left:4px solid #F59E0B;">
            <p style="margin:0;font-family:Arial,sans-serif;font-size:12px;
                      color:#92400E;font-weight:700;">
              &#9888;&nbsp;No active EMR cluster found matching '""" + CLUSTER_NAME + """'.
              Jenkins pipeline may not have triggered today.
              Verify Jenkins job status immediately.
            </p>
          </td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html lang="en"
      xmlns="http://www.w3.org/1999/xhtml"
      xmlns:v="urn:schemas-microsoft-com:vml"
      xmlns:o="urn:schemas-microsoft-com:office:office">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <meta http-equiv="X-UA-Compatible" content="IE=edge">
  <meta name="x-apple-disable-message-reformatting">
  <!--[if mso]>
  <noscript>
    <xml>
      <o:OfficeDocumentSettings>
        <o:AllowPNG/>
        <o:PixelsPerInch>96</o:PixelsPerInch>
      </o:OfficeDocumentSettings>
    </xml>
  </noscript>
  <![endif]-->
  <title>EMR Health Report &mdash; {status}</title>
</head>
<body style="margin:0;padding:0;background-color:#E2E8F0;">

<!-- ── Outer wrapper ── -->
<table cellpadding="0" cellspacing="0" border="0" width="100%"
       style="background-color:#E2E8F0;">
  <tr>
    <td align="center" style="padding:28px 12px;">

      <!-- Page title -->
      <table cellpadding="0" cellspacing="0" border="0" width="640"
             style="max-width:640px;">
        <tr>
          <td align="center" style="padding-bottom:10px;">
            <p style="margin:0;font-family:Arial,sans-serif;font-size:10px;
                      font-weight:700;color:#94A3B8;letter-spacing:2px;
                      text-transform:uppercase;">
              MWAA &middot; AWS EMR Health Report
            </p>
          </td>
        </tr>
      </table>

      <!-- ── Email card ── -->
      <table cellpadding="0" cellspacing="0" border="0" width="640"
             style="max-width:640px;background-color:#FFFFFF;
                    border:1px solid #E2E8F0;">

        <!-- Top accent bar -->
        <tr>
          <td height="5" style="background-color:{accent};
                                font-size:0;line-height:0;">&nbsp;</td>
        </tr>

        <!-- ── HEADER ── -->
        <tr>
          <td style="padding:26px 28px 20px;background-color:{accent_bg};
                     border-bottom:1px solid {accent_border};">
            <table cellpadding="0" cellspacing="0" border="0" width="100%">
              <tr>
                <!-- Status icon -->
                <td style="width:52px;vertical-align:top;">
                  <table cellpadding="0" cellspacing="0" border="0">
                    <tr>
                      <td width="46" height="46" align="center" valign="middle"
                          style="background-color:{accent};
                                 font-family:Arial,sans-serif;
                                 font-size:22px;font-weight:900;
                                 color:#FFFFFF;">
                        {status_icon}
                      </td>
                    </tr>
                  </table>
                </td>
                <!-- Cluster name + status badge -->
                <td style="padding-left:14px;vertical-align:top;">
                  <p style="margin:0 0 4px;font-family:Arial,sans-serif;
                            font-size:10px;font-weight:700;color:{accent};
                            letter-spacing:1.5px;text-transform:uppercase;">
                    AWS EMR &middot; Health Report
                  </p>
                  <p style="margin:0;font-family:Arial,sans-serif;
                            font-size:20px;font-weight:800;color:#0F172A;">
                    {CLUSTER_NAME}
                    &nbsp;
                    <span style="font-size:11px;font-weight:700;
                                 color:{accent};background-color:#FFFFFF;
                                 border:1.5px solid {accent_border};
                                 padding:3px 10px;">
                      {status}
                    </span>
                  </p>
                </td>
              </tr>
            </table>

            <!-- Meta info grid -->
            <table cellpadding="0" cellspacing="0" border="0"
                   width="100%" style="margin-top:18px;">
              <tr>
                <td style="font-family:Arial,sans-serif;font-size:12px;
                           color:#64748B;padding-bottom:5px;width:50%;">
                  <span style="font-weight:700;color:#0F172A;">Report Time:</span>
                  {report_time}
                </td>
                <td style="font-family:Arial,sans-serif;font-size:12px;
                           color:#64748B;padding-bottom:5px;width:50%;">
                  <span style="font-weight:700;color:#0F172A;">Region:</span>
                  {AWS_REGION}
                </td>
              </tr>
              <tr>
                <td style="font-family:Arial,sans-serif;font-size:12px;
                           color:#64748B;padding-bottom:5px;">
                  <span style="font-weight:700;color:#0F172A;">Cluster ID:</span>
                  {cluster_id}
                </td>
                <td style="font-family:Arial,sans-serif;font-size:12px;
                           color:#64748B;padding-bottom:5px;">
                  <span style="font-weight:700;color:#0F172A;">Created:</span>
                  {cluster_created}
                </td>
              </tr>
              <tr>
                <td colspan="2" style="font-family:Arial,sans-serif;
                                       font-size:12px;color:#64748B;">
                  <span style="font-weight:700;color:#0F172A;">Cluster State:</span>
                  &nbsp;
                  <span style="font-weight:700;color:{accent};">
                    {cluster_state}
                  </span>
                </td>
              </tr>
            </table>
          </td>
        </tr>

        <!-- ── SMS BANNER ── -->
        {sms_banner}

        <!-- ── CLUSTER MISSING BANNER ── -->
        {cluster_missing_banner}

        <!-- ── BODY ── -->
        <tr>
          <td style="padding:24px 28px;">

            <!-- Health rule callout -->
            <table cellpadding="0" cellspacing="0" border="0" width="100%"
                   style="background-color:#F8FAFC;border:1px solid #E2E8F0;
                          margin-bottom:22px;">
              <tr>
                <td style="padding:12px 16px;font-family:Arial,sans-serif;
                           font-size:12px;color:#64748B;line-height:1.6;">
                  &#128204;&nbsp;
                  <strong style="color:#0F172A;">Health rule:</strong>
                  Only <strong>RUNNING</strong> nodes count toward the
                  minimum threshold. Nodes in BOOTSTRAPPING, TERMINATING
                  or PENDING states are ignored completely.
                  Min and max values are read dynamically from the
                  EMR auto-scaling policy at runtime.
                </td>
              </tr>
            </table>

            <!-- ── NODE GROUPS TABLE ── -->
            <p style="margin:0 0 10px;font-family:Arial,sans-serif;
                      font-size:10px;font-weight:700;color:#94A3B8;
                      letter-spacing:1px;text-transform:uppercase;">
              Node Groups
            </p>
            <table cellpadding="0" cellspacing="0" border="0" width="100%"
                   style="border:1px solid #E2E8F0;margin-bottom:24px;">
              <thead>
                <tr style="background-color:#F8FAFC;">
                  <th style="padding:9px 14px;text-align:left;
                             font-family:Arial,sans-serif;font-size:10px;
                             font-weight:700;color:#94A3B8;
                             text-transform:uppercase;letter-spacing:0.8px;">
                    Group
                  </th>
                  <th style="padding:9px 14px;text-align:center;
                             font-family:Arial,sans-serif;font-size:10px;
                             font-weight:700;color:#94A3B8;
                             text-transform:uppercase;letter-spacing:0.8px;">
                    Running
                  </th>
                  <th style="padding:9px 14px;text-align:center;
                             font-family:Arial,sans-serif;font-size:10px;
                             font-weight:700;color:#94A3B8;
                             text-transform:uppercase;letter-spacing:0.8px;">
                    Min / Max
                  </th>
                  <th style="padding:9px 14px;font-family:Arial,sans-serif;
                             font-size:10px;font-weight:700;color:#94A3B8;
                             text-transform:uppercase;letter-spacing:0.8px;">
                    Capacity
                  </th>
                  <th style="padding:9px 14px;font-family:Arial,sans-serif;
                             font-size:10px;font-weight:700;color:#94A3B8;
                             text-transform:uppercase;letter-spacing:0.8px;">
                    Detail
                  </th>
                  <th style="padding:9px 14px;text-align:center;
                             font-family:Arial,sans-serif;font-size:10px;
                             font-weight:700;color:#94A3B8;
                             text-transform:uppercase;letter-spacing:0.8px;">
                    Status
                  </th>
                </tr>
              </thead>
              <tbody>
                {node_rows}
              </tbody>
            </table>

            <!-- ── HEALTH CHECKS TABLE ── -->
            <p style="margin:0 0 10px;font-family:Arial,sans-serif;
                      font-size:10px;font-weight:700;color:#94A3B8;
                      letter-spacing:1px;text-transform:uppercase;">
              Health Check Details
            </p>
            <table cellpadding="0" cellspacing="0" border="0" width="100%"
                   style="border:1px solid #E2E8F0;margin-bottom:24px;">
              <thead>
                <tr style="background-color:#F8FAFC;">
                  <th style="padding:9px 14px;text-align:left;
                             font-family:Arial,sans-serif;font-size:10px;
                             font-weight:700;color:#94A3B8;
                             text-transform:uppercase;letter-spacing:0.8px;">
                    Check
                  </th>
                  <th style="padding:9px 14px;text-align:center;
                             font-family:Arial,sans-serif;font-size:10px;
                             font-weight:700;color:#94A3B8;
                             text-transform:uppercase;letter-spacing:0.8px;
                             width:70px;">
                    Result
                  </th>
                  <th style="padding:9px 14px;text-align:left;
                             font-family:Arial,sans-serif;font-size:10px;
                             font-weight:700;color:#94A3B8;
                             text-transform:uppercase;letter-spacing:0.8px;">
                    Detail
                  </th>
                </tr>
              </thead>
              <tbody>
                {check_rows}
              </tbody>
            </table>

            <!-- ── AIRFLOW RUN INFO ── -->
            <table cellpadding="0" cellspacing="0" border="0" width="100%"
                   style="background-color:#F8FAFC;border:1px solid #E2E8F0;">
              <tr>
                <td style="padding:14px 16px;">
                  <p style="margin:0 0 8px;font-family:Arial,sans-serif;
                            font-size:10px;font-weight:700;color:#94A3B8;
                            letter-spacing:1px;text-transform:uppercase;">
                    Airflow Run Info
                  </p>
                  <p style="margin:0 0 4px;font-family:Arial,sans-serif;
                            font-size:12px;color:#64748B;">
                    <strong style="color:#0F172A;">DAG ID:</strong> {dag_id}
                  </p>
                  <p style="margin:0;font-family:Arial,sans-serif;
                            font-size:12px;color:#64748B;">
                    <strong style="color:#0F172A;">Run ID:</strong> {run_id}
                  </p>
                </td>
              </tr>
            </table>

          </td>
        </tr>

        <!-- ── FOOTER ── -->
        <tr>
          <td style="padding:14px 28px;background-color:#F8FAFC;
                     border-top:1px solid #E2E8F0;">
            <table cellpadding="0" cellspacing="0" border="0" width="100%">
              <tr>
                <td style="font-family:Arial,sans-serif;font-size:11px;
                           color:#94A3B8;">
                  Generated by MWAA &middot; AWS {AWS_REGION}
                </td>
                <td align="right" style="font-family:Arial,sans-serif;
                                         font-size:11px;color:#94A3B8;">
                  Automated alert &mdash; do not reply
                </td>
              </tr>
            </table>
          </td>
        </tr>

      </table>
      <!-- end card -->

    </td>
  </tr>
</table>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────────
# Notification data collector
# Used by both email tasks and SMS task
# ─────────────────────────────────────────────────────────────────
def _collect_notification_data(ti, dag_id: str, run_id: str, status: str) -> dict:
    """
    Pull all XCom values and assemble the full data dict
    needed to render the email and SMS.
    """
    cluster_id      = ti.xcom_pull(key=XCOM_CLUSTER_ID,      task_ids="check_cluster_exists") or "NOT_FOUND"
    cluster_state   = ti.xcom_pull(key=XCOM_CLUSTER_STATE,   task_ids="check_cluster_exists") or "UNKNOWN"
    cluster_created = ti.xcom_pull(key=XCOM_CLUSTER_CREATED, task_ids="check_cluster_exists") or "N/A"

    report_time = datetime.now(timezone.utc).strftime("%d %b %Y, %I:%M %p UTC")

    def pull(xcom_key, node_task):
        raw = ti.xcom_pull(key=xcom_key, task_ids=node_task)
        if not raw:
            raw = ti.xcom_pull(key=xcom_key, task_ids="check_cluster_exists")
        return json.loads(raw) if raw else {}

    primary = pull(XCOM_PRIMARY_RESULT, "check_primary_nodes")
    core    = pull(XCOM_CORE_RESULT,    "check_core_nodes")
    task    = pull(XCOM_TASK_RESULT,    "check_task_nodes")

    checks = [
        {
            "label":  "Cluster exists today",
            "result": "FAIL" if cluster_id == "NOT_FOUND" else "PASS",
            "detail": (
                f"No cluster found with name '{CLUSTER_NAME}' — Jenkins may have failed"
                if cluster_id == "NOT_FOUND"
                else f"Cluster found | Created: {cluster_created}"
            ),
        },
        {
            "label":  "Cluster state",
            "result": "PASS" if cluster_state in ("WAITING", "RUNNING") else "FAIL",
            "detail": f"State: {cluster_state}",
        },
        {
            "label":  "Primary — running vs min",
            "result": primary.get("verdict", "FAIL"),
            "detail": primary.get("note", "N/A"),
        },
        {
            "label":  "Core — running vs min",
            "result": core.get("verdict", "FAIL"),
            "detail": core.get("note", "N/A"),
        },
        {
            "label":  "Task — running vs min",
            "result": task.get("verdict", "FAIL"),
            "detail": task.get("note", "N/A"),
        },
    ]

    return {
        "status":          status,
        "cluster_id":      cluster_id,
        "cluster_state":   cluster_state,
        "cluster_created": cluster_created,
        "report_time":     report_time,
        "dag_id":          dag_id,
        "run_id":          run_id,
        "nodes":           [primary, core, task],
        "checks":          checks,
    }


# ─────────────────────────────────────────────────────────────────
# Task 6a — send_healthy_email
# ─────────────────────────────────────────────────────────────────
def send_healthy_email(**context) -> None:
    """Send HEALTHY HTML email via SES. No SMS."""
    ti     = context["ti"]
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    data   = _collect_notification_data(ti, dag_id, run_id, "HEALTHY")

    html = build_email_html(
        status          = "HEALTHY",
        cluster_id      = data["cluster_id"],
        cluster_state   = data["cluster_state"],
        cluster_created = data["cluster_created"],
        nodes           = data["nodes"],
        checks          = data["checks"],
        dag_id          = data["dag_id"],
        run_id          = data["run_id"],
        report_time     = data["report_time"],
        sms_fired       = False,
    )

    subject = (
        f"[EMR] HEALTHY | {CLUSTER_NAME} | {data['report_time']}"
    )

    get_ses_client().send_email(
        Source      = SES_SENDER,
        Destination = {"ToAddresses": SES_RECIPIENTS},
        Message     = {
            "Subject": {"Data": subject, "Charset": "UTF-8"},
            "Body": {
                "Html": {"Data": html,   "Charset": "UTF-8"},
                "Text": {
                    "Data": (
                        f"EMR Health Report — HEALTHY\n"
                        f"Cluster : {CLUSTER_NAME}\n"
                        f"ID      : {data['cluster_id']}\n"
                        f"State   : {data['cluster_state']}\n"
                        f"Time    : {data['report_time']}\n"
                        f"All node groups meet the minimum running threshold."
                    ),
                    "Charset": "UTF-8",
                },
            },
        },
    )
    log.info("HEALTHY email sent | Recipients: %s", SES_RECIPIENTS)


# ─────────────────────────────────────────────────────────────────
# Task 6b — send_unhealthy_email
# ─────────────────────────────────────────────────────────────────
def send_unhealthy_email(**context) -> None:
    """Send UNHEALTHY HTML email via SES."""
    ti     = context["ti"]
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    data   = _collect_notification_data(ti, dag_id, run_id, "UNHEALTHY")

    html = build_email_html(
        status          = "UNHEALTHY",
        cluster_id      = data["cluster_id"],
        cluster_state   = data["cluster_state"],
        cluster_created = data["cluster_created"],
        nodes           = data["nodes"],
        checks          = data["checks"],
        dag_id          = data["dag_id"],
        run_id          = data["run_id"],
        report_time     = data["report_time"],
        sms_fired       = True,
    )

    subject = (
        f"[EMR] UNHEALTHY | {CLUSTER_NAME} | {data['report_time']}"
    )

    get_ses_client().send_email(
        Source      = SES_SENDER,
        Destination = {"ToAddresses": SES_RECIPIENTS},
        Message     = {
            "Subject": {"Data": subject, "Charset": "UTF-8"},
            "Body": {
                "Html": {"Data": html,   "Charset": "UTF-8"},
                "Text": {
                    "Data": (
                        f"EMR Health Report — UNHEALTHY\n"
                        f"Cluster : {CLUSTER_NAME}\n"
                        f"ID      : {data['cluster_id']}\n"
                        f"State   : {data['cluster_state']}\n"
                        f"Time    : {data['report_time']}\n"
                        f"ACTION REQUIRED — check node groups and Jenkins pipeline."
                    ),
                    "Charset": "UTF-8",
                },
            },
        },
    )
    log.info("UNHEALTHY email sent | Recipients: %s", SES_RECIPIENTS)


# ─────────────────────────────────────────────────────────────────
# Task 7 — send_sms_alert
# ─────────────────────────────────────────────────────────────────
def send_sms_alert(**context) -> None:
    """
    Send brief SMS via SNS on UNHEALTHY only.
    Kept concise — cluster name, region, failed groups,
    deficit counts and recommended action.
    """
    ti   = context["ti"]
    data = _collect_notification_data(
        ti, context["dag"].dag_id, context["run_id"], "UNHEALTHY"
    )

    is_missing = data["cluster_id"] == "NOT_FOUND"

    if is_missing:
        message = (
            f"[EMR ALERT] UNHEALTHY\n"
            f"Cluster : {CLUSTER_NAME}\n"
            f"Region  : {AWS_REGION}\n"
            f"Issue   : Cluster not found\n"
            f"Action  : Check Jenkins pipeline"
        )
    else:
        failed_lines = [
            f"  {n['display_name']}: {n['running']} running / {n['min']} min (deficit {n['deficit']})"
            for n in data["nodes"]
            if n.get("verdict") == "FAIL"
        ]
        message = (
            f"[EMR ALERT] UNHEALTHY\n"
            f"Cluster : {CLUSTER_NAME}\n"
            f"Region  : {AWS_REGION}\n"
            f"State   : {data['cluster_state']}\n"
            f"Failed  :\n" + "\n".join(failed_lines) + "\n"
            f"Action  : Check EMR console"
        )

    get_sns_client().publish(
        TopicArn = SNS_TOPIC_ARN,
        Message  = message,
        Subject  = f"EMR UNHEALTHY — {CLUSTER_NAME}",
    )
    log.info("SMS dispatched | Topic: %s", SNS_TOPIC_ARN)


# ─────────────────────────────────────────────────────────────────
# DAG Definition
# ─────────────────────────────────────────────────────────────────
default_args = {
    "owner":            "etl-support",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          0,       # No retries on any task — fail fast
}

with DAG(
    dag_id            = "emr_health_check_dag",
    description       = "Daily EMR cluster health check — SES email + SNS SMS alerts",
    default_args      = default_args,
    start_date        = days_ago(1),
    schedule_interval = "0 6 * * *",  # 06:00 AM UTC daily
    catchup           = False,
    max_active_runs   = 1,
    tags              = ["emr", "health-check", "monitoring", "etl"],
) as dag:

    # ── Task 0: Start ─────────────────────────────────────────────
    start = EmptyOperator(task_id="start")

    # ── Task 1: Check cluster exists ──────────────────────────────
    t_check_exists = PythonOperator(
        task_id         = "check_cluster_exists",
        python_callable = check_cluster_exists,
        retries         = 0,
    )

    # ── Task 2: Check primary nodes ───────────────────────────────
    t_check_primary = PythonOperator(
        task_id         = "check_primary_nodes",
        python_callable = check_primary_nodes,
        retries         = 0,
    )

    # ── Task 3: Check core nodes ──────────────────────────────────
    t_check_core = PythonOperator(
        task_id         = "check_core_nodes",
        python_callable = check_core_nodes,
        retries         = 0,
    )

    # ── Task 4: Check task nodes ──────────────────────────────────
    t_check_task = PythonOperator(
        task_id         = "check_task_nodes",
        python_callable = check_task_nodes,
        retries         = 0,
    )

    # ── Task 5: Evaluate health (branch) ──────────────────────────
    # TriggerRule.ALL_DONE ensures this runs even when upstream
    # tasks were skipped (cluster missing scenario)
    t_evaluate = BranchPythonOperator(
        task_id         = "evaluate_overall_health",
        python_callable = evaluate_overall_health,
        retries         = 0,
        trigger_rule    = TriggerRule.ALL_DONE,
    )

    # ── Task 6a: Healthy email ────────────────────────────────────
    t_healthy_email = PythonOperator(
        task_id         = "send_healthy_email",
        python_callable = send_healthy_email,
        retries         = 0,
    )

    # ── Task 6b: Unhealthy email ──────────────────────────────────
    t_unhealthy_email = PythonOperator(
        task_id         = "send_unhealthy_email",
        python_callable = send_unhealthy_email,
        retries         = 0,
    )

    # ── Task 7: SMS alert (UNHEALTHY only) ────────────────────────
    t_sms = PythonOperator(
        task_id         = "send_sms_alert",
        python_callable = send_sms_alert,
        retries         = 0,
    )

    # ── Task 8: End ───────────────────────────────────────────────
    # ONE_SUCCESS ensures end runs whichever branch completes
    end = EmptyOperator(
        task_id      = "end",
        trigger_rule = TriggerRule.ONE_SUCCESS,
    )

    # ─────────────────────────────────────────────────────────────
    # DAG flow
    #
    #  start
    #    │
    #    ▼
    #  check_cluster_exists
    #    │
    #    ▼
    #  check_primary_nodes
    #    │
    #    ▼
    #  check_core_nodes
    #    │
    #    ▼
    #  check_task_nodes
    #    │
    #    ▼
    #  evaluate_overall_health  ← TriggerRule.ALL_DONE
    #    │
    #    ├── HEALTHY  ──► send_healthy_email ──────────────────► end
    #    │                                                        ▲
    #    └── UNHEALTHY ──► send_unhealthy_email ──► send_sms ────┘
    #
    # ─────────────────────────────────────────────────────────────

    (
        start
        >> t_check_exists
        >> t_check_primary
        >> t_check_core
        >> t_check_task
        >> t_evaluate
    )

    t_evaluate >> t_healthy_email >> end
    t_evaluate >> t_unhealthy_email >> t_sms >> end
