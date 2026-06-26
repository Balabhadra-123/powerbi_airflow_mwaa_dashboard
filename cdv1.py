"""
==============================================================================
Project      : EMR Cluster Health Monitor
File         : emr_cluster_health_monitor.py
Author       : Balabhadra Patra
Version      : 1.0.0

Description
-----------
This DAG validates the health of the AWS EMR cluster before business Airflow
DAGs begin processing.

Health Rules
------------
1. Active EMR cluster must exist.
2. Primary node must be running.
3. Core running nodes must be greater than or equal to configured minimum.
4. Task running nodes must be greater than or equal to configured minimum.
   Ignored if the Task instance group does not exist.

Notifications
-------------
HEALTHY
    - Email

UNHEALTHY
    - Email
    - SNS SMS

CLUSTER NOT FOUND
    - Email
    - SNS SMS

Compatible With
---------------
Apache Airflow (MWAA) 2.10.3
Python 3.11
==============================================================================
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from html import escape

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email

# ==============================================================================
# Configuration
# ==============================================================================

AWS_REGION = "ap-southeast-2"
CLUSTER_NAME = "REPLACE_CLUSTER_NAME"
EMAIL_RECIPIENTS = ["support@company.com"]
SNS_TOPIC_ARN = "REPLACE_SNS_TOPIC"
ENVIRONMENT = "NON-PROD"

# EMR states considered active.
ACTIVE_CLUSTER_STATES = ["STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING"]

# ==============================================================================
# Airflow Default Arguments
# ==============================================================================

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ==============================================================================
# Status Constants
# ==============================================================================

STATUS_PASS = "PASS"
STATUS_FAIL = "FAIL"
STATUS_HEALTHY = "HEALTHY"
STATUS_UNHEALTHY = "UNHEALTHY"
STATUS_CLUSTER_NOT_FOUND = "CLUSTER_NOT_FOUND"

# ==============================================================================
# Email Colours
# ==============================================================================

GREEN = "#107C41"
RED = "#C50F1F"
GREY = "#F8FAFC"
BORDER = "#D1D5DB"
TEXT = "#222222"
SUB_TEXT = "#666666"
MUTED_TEXT = "#475467"
PASS_BACKGROUND = "#D1FAE5"
PASS_TEXT = "#047857"
FAIL_BACKGROUND = "#FEE2E2"
FAIL_TEXT = "#B91C1C"
EMAIL_FONT = "Segoe UI, Arial, sans-serif"
EMAIL_WIDTH = "720"

# ==============================================================================
# Logger
# ==============================================================================

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ==============================================================================
# Logging Helpers
# ==============================================================================


def log_separator():
    """Writes a separator line to the Airflow log."""
    logger.info("=" * 80)


def log_heading(title: str):
    """Writes a section heading."""
    logger.info("")
    log_separator()
    logger.info(title)
    log_separator()


def log_sub_heading(title: str):
    """Writes a subsection heading."""
    logger.info("")
    logger.info("-" * 80)
    logger.info(title)
    logger.info("-" * 80)


def log_key_value(key: str, value):
    """Logs values in aligned key/value format."""
    logger.info("%-35s : %s", key, value)


# ==============================================================================
# AWS Client Helpers
# ==============================================================================


def get_emr_client():
    """Returns an EMR boto3 client."""
    return boto3.client("emr", region_name=AWS_REGION)


def get_sns_client():
    """Returns an SNS boto3 client."""
    return boto3.client("sns", region_name=AWS_REGION)


# ==============================================================================
# Utility Functions
# ==============================================================================


def get_report_time():
    """Returns report timestamp."""
    return datetime.now().strftime("%d %b %Y %H:%M:%S")


def safe_value(value):
    """Returns an empty string if value is None."""
    return "" if value is None else str(value)


def html_value(value):
    """Escapes dynamic values before placing them in HTML."""
    return escape(safe_value(value))


def get_capacity_constraints(group):
    """Returns min/max capacity with a fallback for non-autoscaled groups."""
    constraints = (group.get("AutoScalingPolicy") or {}).get("Constraints") or {}
    requested_count = group.get("RequestedInstanceCount", group.get("RunningInstanceCount", 0))

    return {
        "minimum": constraints.get("MinCapacity", requested_count),
        "maximum": constraints.get("MaxCapacity", requested_count),
    }


# ==============================================================================
# EMR Helper Functions
# ==============================================================================


def find_active_cluster(emr_client):
    """
    Searches for an active EMR cluster matching CLUSTER_NAME.

    Returns
    -------
    dict | None
        Active cluster dictionary if found, otherwise None.
    """
    log_heading("Searching Active EMR Cluster")

    paginator = emr_client.get_paginator("list_clusters")
    active_cluster_count = 0

    for page in paginator.paginate(ClusterStates=ACTIVE_CLUSTER_STATES):
        clusters = page.get("Clusters", [])
        active_cluster_count += len(clusters)

        for cluster in clusters:
            log_key_value("Checking Cluster", cluster["Name"])

            if cluster["Name"] == CLUSTER_NAME:
                log_key_value("Active Cluster Count", active_cluster_count)
                logger.info("Matching cluster found.")
                return cluster

    log_key_value("Active Cluster Count", active_cluster_count)
    logger.warning("No active cluster found.")
    return None


def describe_cluster(emr_client, cluster_id):
    """Returns detailed EMR cluster information."""
    logger.info("Retrieving cluster details...")
    response = emr_client.describe_cluster(ClusterId=cluster_id)
    return response["Cluster"]


def get_instance_groups(emr_client, cluster_id):
    """
    Returns instance groups keyed by type.

    Example
    -------
    MASTER
    CORE
    TASK
    """
    log_heading("Retrieving Instance Groups")

    paginator = emr_client.get_paginator("list_instance_groups")
    groups = {}

    for page in paginator.paginate(ClusterId=cluster_id):
        for group in page.get("InstanceGroups", []):
            group_type = group["InstanceGroupType"]
            groups[group_type] = group
            log_key_value(f"{group_type} Running Nodes", group["RunningInstanceCount"])

    return groups


def build_node_result(group_name, minimum, maximum, running):
    """Creates a standard node object."""
    status = STATUS_PASS if running >= minimum else STATUS_FAIL
    node = {
        "group": group_name,
        "minimum": minimum,
        "maximum": maximum,
        "running": running,
        "status": status,
    }

    log_sub_heading(f"{group_name} Instance Group")
    log_key_value("Minimum Nodes", minimum)
    log_key_value("Maximum Nodes", maximum)
    log_key_value("Running Nodes", running)
    log_key_value("Status", status)

    return node


def evaluate_cluster_health(cluster, groups):
    """
    Evaluates the health of the EMR cluster.

    Rules
    -----
    Primary
        Running == 1

    Core
        Running >= Minimum

    Task
        Running >= Minimum
        Ignored if Task group not present.
    """
    log_heading("Evaluating Cluster Health")

    missing_required_groups = [group_type for group_type in ("MASTER", "CORE") if group_type not in groups]
    if missing_required_groups:
        raise ValueError(
            "Required EMR instance group(s) missing: "
            + ", ".join(missing_required_groups)
        )

    master_group = groups["MASTER"]
    primary = build_node_result(
        group_name="Primary",
        minimum=1,
        maximum=1,
        running=master_group["RunningInstanceCount"],
    )

    core_group = groups["CORE"]
    core_constraints = get_capacity_constraints(core_group)
    core = build_node_result(
        group_name="Core",
        minimum=core_constraints["minimum"],
        maximum=core_constraints["maximum"],
        running=core_group["RunningInstanceCount"],
    )

    task_group = groups.get("TASK")
    if task_group:
        task_constraints = get_capacity_constraints(task_group)
        task = build_node_result(
            group_name="Task",
            minimum=task_constraints["minimum"],
            maximum=task_constraints["maximum"],
            running=task_group["RunningInstanceCount"],
        )
    else:
        logger.info("Task instance group not configured.")
        task = {
            "group": "Task",
            "minimum": 0,
            "maximum": 0,
            "running": 0,
            "status": STATUS_PASS,
        }

    statuses = [primary["status"], core["status"], task["status"]]
    overall_status = (
        STATUS_HEALTHY
        if all(status == STATUS_PASS for status in statuses)
        else STATUS_UNHEALTHY
    )

    log_separator()
    log_key_value("Overall Cluster Health", overall_status)
    log_separator()

    return {
        "cluster_name": cluster["Name"],
        "cluster_id": cluster["Id"],
        "cluster_state": cluster["Status"]["State"],
        "overall_status": overall_status,
        "report_time": get_report_time(),
        "sms_sent": False,
        "nodes": [primary, core, task],
        "checks": build_health_checks(cluster, primary, core, task, overall_status),
    }


def build_cluster_not_found_result():
    """Returns a standard result object when no active cluster is found."""
    return {
        "cluster_name": CLUSTER_NAME,
        "cluster_id": "N/A",
        "cluster_state": "NOT FOUND",
        "overall_status": STATUS_CLUSTER_NOT_FOUND,
        "report_time": get_report_time(),
        "sms_sent": False,
        "nodes": [],
        "checks": [],
    }


def build_health_checks(cluster, primary, core, task, overall_status):
    """
    Builds the health validation results.

    These checks are reused by:
        - Email
        - Logging
        - Dashboard (future)
    """
    return [
        {
            "name": "Active EMR Cluster",
            "status": STATUS_PASS,
            "actual": cluster["Name"],
        },
        {
            "name": "Cluster State",
            "status": STATUS_PASS,
            "actual": cluster["Status"]["State"],
        },
        {
            "name": "Primary Node Capacity",
            "status": primary["status"],
            "actual": f"Running {primary['running']} / Minimum {primary['minimum']}",
        },
        {
            "name": "Core Node Capacity",
            "status": core["status"],
            "actual": f"Running {core['running']} / Minimum {core['minimum']}",
        },
        {
            "name": "Task Node Capacity",
            "status": task["status"],
            "actual": f"Running {task['running']} / Minimum {task['minimum']}",
        },
        {
            "name": "Overall Cluster Health",
            "status": STATUS_PASS if overall_status == STATUS_HEALTHY else STATUS_FAIL,
            "actual": overall_status,
        },
    ]


def log_health_checks(result):
    """Writes all validation results to the Airflow log."""
    log_heading("Health Check Results")

    for check in result["checks"]:
        logger.info(
            "%-35s %-6s %s",
            check["name"],
            check["status"],
            check["actual"],
        )


def check_emr_health(**context):
    """
    Main EMR validation task.

    Steps
    -----
    1. Connect to EMR
    2. Find active cluster
    3. Read instance groups
    4. Evaluate health
    5. Push result to XCom
    """
    log_heading("EMR HEALTH CHECK STARTED")
    emr = get_emr_client()

    try:
        cluster = find_active_cluster(emr)

        if cluster is None:
            logger.warning("No active EMR cluster found.")
            result = build_cluster_not_found_result()
            result["checks"] = [
                {
                    "name": "Active EMR Cluster",
                    "status": STATUS_FAIL,
                    "actual": "Cluster not found",
                },
                {
                    "name": "Overall Cluster Health",
                    "status": STATUS_FAIL,
                    "actual": STATUS_CLUSTER_NOT_FOUND,
                },
            ]

            context["ti"].xcom_push(key="health_result", value=result)
            log_health_checks(result)
            return result

        cluster_details = describe_cluster(emr, cluster["Id"])

        log_sub_heading("Cluster Details")
        log_key_value("Cluster Name", cluster_details["Name"])
        log_key_value("Cluster ID", cluster_details["Id"])
        log_key_value("Cluster State", cluster_details["Status"]["State"])

        groups = get_instance_groups(emr, cluster["Id"])
        result = evaluate_cluster_health(cluster_details, groups)

        log_health_checks(result)
        context["ti"].xcom_push(key="health_result", value=result)

        log_heading("EMR HEALTH CHECK COMPLETED")
        return result

    except Exception:
        logger.exception("Unexpected error while checking EMR health.")
        raise


# ==============================================================================
# HTML Helpers
# ==============================================================================


def build_badge(status):
    """Returns Outlook compatible PASS / FAIL badge."""
    if status == STATUS_PASS:
        background = PASS_BACKGROUND
        colour = PASS_TEXT
    else:
        background = FAIL_BACKGROUND
        colour = FAIL_TEXT

    return f"""
<span style="
display:inline-block;
padding:4px 10px;
background:{background};
color:{colour};
border-radius:12px;
font-family:{EMAIL_FONT};
font-size:12px;
font-weight:bold;
">
{status}
</span>
"""


def build_table(title, headers, rows):
    """Creates an Outlook compatible HTML table."""
    html = f"""
<h3 style="
font-family:{EMAIL_FONT};
color:{TEXT};
margin-top:25px;
margin-bottom:10px;
font-size:16px;
font-weight:600;
">
{html_value(title)}
</h3>

<table
width="100%"
cellpadding="8"
cellspacing="0"
role="presentation"
style="
border-collapse:collapse;
border:1px solid {BORDER};
font-family:{EMAIL_FONT};
font-size:13px;
color:{TEXT};
">
"""

    html += "<tr>"
    for header in headers:
        html += f"""
<th style="
background:{GREY};
border:1px solid {BORDER};
color:{MUTED_TEXT};
font-size:12px;
font-weight:bold;
text-align:left;
">
{html_value(header)}
</th>
"""

    html += "</tr>"
    alternate = False

    for row in rows:
        colour = "#FAFAFA" if alternate else "#FFFFFF"
        alternate = not alternate
        html += f'<tr style="background:{colour};">'

        for value in row:
            html += f"""
<td style="
border:1px solid {BORDER};
vertical-align:top;
">
{value}
</td>
"""

        html += "</tr>"

    html += "</table>"
    return html


def validation_summary_rows(result):
    passed = len([c for c in result["checks"] if c["status"] == STATUS_PASS])
    failed = len(result["checks"]) - passed

    return [
        ["Overall Status", result["overall_status"]],
        ["Checks Passed", passed],
        ["Checks Failed", failed],
        ["SMS Triggered", "Yes" if result["sms_sent"] else "No"],
    ]


def cluster_information_rows(result):
    return [
        ["Cluster Name", html_value(result["cluster_name"])],
        ["Cluster ID", html_value(result["cluster_id"])],
        ["Cluster State", html_value(result["cluster_state"])],
    ]


def node_capacity_rows(result):
    rows = []

    for node in result["nodes"]:
        rows.append(
            [
                node["group"],
                node["minimum"],
                node["maximum"],
                node["running"],
                build_badge(node["status"]),
            ]
        )

    return rows


def health_check_rows(result):
    rows = []

    for check in result["checks"]:
        rows.append(
            [
                check["name"],
                build_badge(check["status"]),
                html_value(check["actual"]),
            ]
        )

    return rows


def recommended_notes(result):
    if result["overall_status"] == STATUS_HEALTHY:
        return []

    if result["overall_status"] == STATUS_CLUSTER_NOT_FOUND:
        return [
            "Review the Jenkins EMR creation pipeline if this alert is unexpected.",
            "Confirm the EMR cluster exists in AWS and was created before the MWAA health check ran.",
            "Check EMR bootstrap logs if the cluster was created but did not reach an active state.",
        ]

    return [
        "Review the Jenkins EMR creation pipeline if this alert is unexpected.",
        "Confirm the EMR cluster exists in AWS and review Core or Task instance group capacity.",
        "Check EMR bootstrap logs if node capacity remains below the expected minimum.",
    ]


def build_notes(notes):
    """Creates a quiet Outlook compatible notes section."""
    if not notes:
        return ""

    note_html = "".join(
        f"""
<p style="
margin:0 0 7px 0;
font-family:{EMAIL_FONT};
font-size:13px;
line-height:20px;
color:{MUTED_TEXT};
">
{html_value(note)}
</p>
"""
        for note in notes
    )

    return f"""
<h3 style="
font-family:{EMAIL_FONT};
color:{TEXT};
margin-top:25px;
margin-bottom:10px;
font-size:16px;
font-weight:600;
">
Notes
</h3>

<table
width="100%"
cellpadding="12"
cellspacing="0"
role="presentation"
style="
border-collapse:collapse;
background:#F8FAFC;
border:1px solid #E4E7EC;
font-family:{EMAIL_FONT};
">
<tr>
<td style="
border:1px solid #E4E7EC;
">
{note_html}
</td>
</tr>
</table>
"""


def email_status_details(result):
    """Returns subject and banner details for the current health status."""
    cluster_name = result["cluster_name"]

    if result["overall_status"] == STATUS_HEALTHY:
        return {
            "symbol": "✓",
            "label": "Healthy",
            "banner_colour": GREEN,
            "summary": (
                "The EMR cluster is healthy. All instance groups satisfy the "
                "configured minimum running node requirements."
            ),
            "heading": f"EMR {cluster_name} Health Report - Healthy",
        }

    if result["overall_status"] == STATUS_CLUSTER_NOT_FOUND:
        return {
            "symbol": "✕",
            "label": "Cluster Not Found",
            "banner_colour": RED,
            "summary": (
                "No active EMR cluster was found. Verify the Jenkins EMR creation "
                "pipeline and confirm the cluster has been created."
            ),
            "heading": f"EMR {cluster_name} Health Report - Cluster Not Found",
        }

    return {
        "symbol": "✕",
        "label": "Unhealthy",
        "banner_colour": RED,
        "summary": (
            "The EMR cluster is unhealthy. One or more instance groups are below "
            "the configured minimum running node count."
        ),
        "heading": f"EMR {cluster_name} Health Report - Unhealthy",
    }


def build_email_subject(result):
    """Builds the email subject."""
    details = email_status_details(result)
    timestamp = result["report_time"]
    return f"{details['symbol']} {details['heading']} | {timestamp}"


def build_email(result, context=None):
    """Builds the Outlook compatible HTML email."""
    details = email_status_details(result)

    html = f"""
<!DOCTYPE html>

<html>

<head>
<meta charset="UTF-8">
<title>EMR Health Report</title>
</head>

<body style="
background:#EEF2F7;
padding:20px;
margin:0;
font-family:{EMAIL_FONT};
">

<table
width="{EMAIL_WIDTH}"
align="center"
cellpadding="0"
cellspacing="0"
role="presentation"
style="
background:#FFFFFF;
border:1px solid {BORDER};
font-family:{EMAIL_FONT};
">

<tr>
<td style="
padding:0;
">

<table
width="100%"
cellpadding="0"
cellspacing="0"
role="presentation"
style="
border-collapse:collapse;
background:{details["banner_colour"]};
">
<tr>
<td style="
padding:24px 28px;
color:white;
">
<p style="
margin:0 0 8px 0;
font-family:{EMAIL_FONT};
font-size:13px;
font-weight:bold;
letter-spacing:0.4px;
text-transform:uppercase;
color:#FFFFFF;
">
{details["symbol"]} {html_value(details["label"])}
</p>
<p style="
margin:0;
font-family:{EMAIL_FONT};
font-size:24px;
font-weight:bold;
line-height:32px;
color:#FFFFFF;
">
{html_value(details["heading"])}
</p>
</td>
</tr>
</table>

<table
width="100%"
cellpadding="22"
cellspacing="0"
role="presentation"
style="
border-collapse:collapse;
">
<tr>
<td>

<p style="
font-size:15px;
margin:0 0 20px 0;
line-height:24px;
color:{TEXT};
font-family:{EMAIL_FONT};
">
{html_value(details["summary"])}
</p>
"""

    html += build_table(
        "Cluster Information",
        ["Property", "Value"],
        cluster_information_rows(result),
    )

    if result["overall_status"] != STATUS_CLUSTER_NOT_FOUND:
        html += build_table(
            "Node Capacity",
            ["Group", "Minimum", "Maximum", "Running", "Status"],
            node_capacity_rows(result),
        )

    html += build_table(
        "Health Check Results",
        ["Validation", "Result", "Actual Value"],
        health_check_rows(result),
    )

    html += build_notes(recommended_notes(result))

    html += f"""

<p style="
font-size:12px;
color:{SUB_TEXT};
line-height:20px;
border-top:1px solid {BORDER};
margin-top:30px;
padding-top:16px;
font-family:{EMAIL_FONT};
">
This is an automated notification generated by Apache Airflow (MWAA).
</p>

</td>
</tr>
</table>

</td>
</tr>
</table>
</body>
</html>
"""

    return html


def send_email_notification(result, context):
    """
    Sends the HTML email using MWAA SMTP configuration.

    Returns
    -------
    bool
        True if email sent successfully.
    """
    log_heading("Sending Email Notification")

    subject = build_email_subject(result)
    html_body = build_email(result=result, context=context)

    log_key_value("Recipients", ", ".join(EMAIL_RECIPIENTS))
    log_key_value("Subject", subject)

    try:
        send_email(
            to=EMAIL_RECIPIENTS,
            subject=subject,
            html_content=html_body,
        )
        logger.info("Email sent successfully.")
        return True

    except Exception:
        logger.exception("Failed to send email.")
        return False


def send_sms_notification(result):
    """
    Sends SMS notification via SNS.

    SMS is only sent for:
        - UNHEALTHY
        - CLUSTER_NOT_FOUND

    Returns
    -------
    bool
    """
    if result["overall_status"] == STATUS_HEALTHY:
        logger.info("Healthy cluster. SMS notification skipped.")
        return True

    log_heading("Sending SNS Notification")
    sns = get_sns_client()

    if result["overall_status"] == STATUS_CLUSTER_NOT_FOUND:
        message = (
            f"CRITICAL: EMR Cluster '{CLUSTER_NAME}' was not found. "
            "Please check your email."
        )
    else:
        message = (
            f"CRITICAL: EMR Cluster '{CLUSTER_NAME}' is unhealthy. "
            "Please check your email."
        )

    try:
        response = sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="EMR Health Alert",
            Message=message,
        )

        result["sms_sent"] = True
        log_key_value("SNS Message ID", response.get("MessageId"))
        logger.info("SMS sent successfully.")
        return True

    except Exception:
        logger.exception("Failed to send SMS.")
        return False


def send_notifications(**context):
    """Sends Email and SMS notifications using the health result from XCom."""
    log_heading("Notification Engine Started")

    ti = context["ti"]
    result = ti.xcom_pull(task_ids="check_emr_health", key="health_result")

    if result is None:
        raise ValueError("Health result not found in XCom.")

    sms_success = send_sms_notification(result)
    email_success = send_email_notification(result, context)

    log_heading("Notification Summary")
    log_key_value("Overall Status", result["overall_status"])
    log_key_value("Email Sent", email_success)
    log_key_value("SMS Sent", sms_success)

    if not email_success and not sms_success:
        raise Exception("Both Email and SMS notifications failed.")

    logger.info("Notification Engine Completed Successfully.")


# ==============================================================================
# DAG Definition
# ==============================================================================

with DAG(
    dag_id="emr_cluster_health_monitor",
    description="AWS EMR Cluster Health Monitoring",
    default_args=default_args,
    schedule="30 06 * * *",  # Update according to Jenkins schedule.
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["AWS", "EMR", "Monitoring", "Production"],
) as dag:
    health_check = PythonOperator(
        task_id="check_emr_health",
        python_callable=check_emr_health,
    )

    notifications = PythonOperator(
        task_id="send_notifications",
        python_callable=send_notifications,
    )

    health_check >> notifications
