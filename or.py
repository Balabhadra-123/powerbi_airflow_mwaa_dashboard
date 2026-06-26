"""
==============================================================================
Project      : EMR Cluster Health Monitor
File         : emr_cluster_health_monitor.py
Author       : Balabhadra Patra
Version      : 1.0.0

Description
-----------
This DAG validates the health of the AWS EMR cluster before business
Airflow DAGs begin processing.

Health Rules
------------
1. Active EMR cluster must exist.
2. Primary node must be running.
3. Core running nodes must be greater than or equal to configured minimum.
4. Task running nodes must be greater than or equal to configured minimum.
   (Ignored if Task instance group does not exist.)

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

# ==============================================================================
# Imports
# ==============================================================================

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import boto3

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email

# ==============================================================================
# Configuration
# ==============================================================================

AWS_REGION = "ap-southeast-2"

CLUSTER_NAME = "REPLACE_CLUSTER_NAME"

EMAIL_RECIPIENTS = [
    "support@company.com"
]

SNS_TOPIC_ARN = "REPLACE_SNS_TOPIC"

ENVIRONMENT = "NON-PROD"

# EMR states considered active
ACTIVE_CLUSTER_STATES = [
    "STARTING",
    "BOOTSTRAPPING",
    "RUNNING",
    "WAITING"
]

# ==============================================================================
# Airflow Default Arguments
# ==============================================================================

default_args = {

    "owner": "airflow",

    "depends_on_past": False,

    "retries": 1,

    "retry_delay": timedelta(minutes=5)

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

GREY = "#F4F6F9"

BORDER = "#D1D5DB"

TEXT = "#222222"

SUB_TEXT = "#666666"

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
    return boto3.client(
        "emr",
        region_name=AWS_REGION
    )


def get_sns_client():
    """Returns an SNS boto3 client."""
    return boto3.client(
        "sns",
        region_name=AWS_REGION
    )

# ==============================================================================
# Utility Functions
# ==============================================================================

def get_report_time():
    """Returns report timestamp."""
    return datetime.now().strftime("%d %b %Y %H:%M:%S")


def safe_value(value):
    """Returns an empty string if value is None."""
    return "" if value is None else str(value)

# ==============================================================================
# EMR HELPER FUNCTIONS
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

    response = emr_client.list_clusters(
        ClusterStates=ACTIVE_CLUSTER_STATES
    )

    clusters = response.get("Clusters", [])

    log_key_value("Active Cluster Count", len(clusters))

    for cluster in clusters:

        log_key_value("Checking Cluster", cluster["Name"])

        if cluster["Name"] == CLUSTER_NAME:

            logger.info("Matching cluster found.")

            return cluster

    logger.warning("No active cluster found.")

    return None


# ==============================================================================
# Describe Cluster
# ==============================================================================

def describe_cluster(emr_client, cluster_id):
    """
    Returns detailed EMR cluster information.
    """

    logger.info("Retrieving cluster details...")

    response = emr_client.describe_cluster(
        ClusterId=cluster_id
    )

    return response["Cluster"]


# ==============================================================================
# Instance Groups
# ==============================================================================

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

    response = emr_client.list_instance_groups(
        ClusterId=cluster_id
    )

    groups = {}

    for group in response["InstanceGroups"]:

        group_type = group["InstanceGroupType"]

        groups[group_type] = group

        log_key_value(
            f"{group_type} Running Nodes",
            group["RunningInstanceCount"]
        )

    return groups


# ==============================================================================
# Build Node Result
# ==============================================================================

def build_node_result(
    group_name,
    minimum,
    maximum,
    running
):
    """
    Creates a standard node object.
    """

    status = (
        STATUS_PASS
        if running >= minimum
        else STATUS_FAIL
    )

    node = {

        "group": group_name,

        "minimum": minimum,

        "maximum": maximum,

        "running": running,

        "status": status

    }

    log_sub_heading(f"{group_name} Instance Group")

    log_key_value("Minimum Nodes", minimum)

    log_key_value("Maximum Nodes", maximum)

    log_key_value("Running Nodes", running)

    log_key_value("Status", status)

    return node


# ==============================================================================
# Evaluate Cluster Health
# ==============================================================================

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

    # ----------------------------------------------------------------------
    # PRIMARY
    # ----------------------------------------------------------------------

    master_group = groups["MASTER"]

    primary = build_node_result(

        group_name="Primary",

        minimum=1,

        maximum=1,

        running=master_group["RunningInstanceCount"]

    )

    # ----------------------------------------------------------------------
    # CORE
    # ----------------------------------------------------------------------

    core_group = groups["CORE"]

    core_constraints = core_group["AutoScalingPolicy"]["Constraints"]

    core = build_node_result(

        group_name="Core",

        minimum=core_constraints["MinCapacity"],

        maximum=core_constraints["MaxCapacity"],

        running=core_group["RunningInstanceCount"]

    )

    # ----------------------------------------------------------------------
    # TASK
    # ----------------------------------------------------------------------

    task_group = groups.get("TASK")

    if task_group:

        task_constraints = task_group["AutoScalingPolicy"]["Constraints"]

        task = build_node_result(

            group_name="Task",

            minimum=task_constraints["MinCapacity"],

            maximum=task_constraints["MaxCapacity"],

            running=task_group["RunningInstanceCount"]

        )

    else:

        logger.info(
            "Task instance group not configured."
        )

        task = {

            "group": "Task",

            "minimum": 0,

            "maximum": 0,

            "running": 0,

            "status": STATUS_PASS

        }

    # ----------------------------------------------------------------------
    # OVERALL STATUS
    # ----------------------------------------------------------------------

    statuses = [

        primary["status"],

        core["status"],

        task["status"]

    ]

    overall_status = (

        STATUS_HEALTHY

        if all(status == STATUS_PASS for status in statuses)

        else STATUS_UNHEALTHY

    )

    log_separator()

    log_key_value(
        "Overall Cluster Health",
        overall_status
    )

    log_separator()

    return {

        "cluster_name": cluster["Name"],

        "cluster_id": cluster["Id"],

        "cluster_state": cluster["Status"]["State"],

        "overall_status": overall_status,

        "report_time": get_report_time(),

        "sms_sent": False,

        "nodes": [

            primary,

            core,

            task

        ],

"checks": build_health_checks(
    cluster,
    primary,
    core,
    task,
    overall_status
)

    }


# ==============================================================================
# Cluster Not Found Result
# ==============================================================================

def build_cluster_not_found_result():
    """
    Returns a standard result object when
    no active cluster is found.
    """

    return {

        "cluster_name": CLUSTER_NAME,

        "cluster_id": "N/A",

        "cluster_state": "NOT FOUND",

        "overall_status": STATUS_CLUSTER_NOT_FOUND,

        "report_time": get_report_time(),

        "sms_sent": False,

        "nodes": [],

        "checks": []

    }

# ==============================================================================
# HEALTH CHECK DETAILS
# ==============================================================================

def build_health_checks(
    cluster,
    primary,
    core,
    task,
    overall_status
):
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
            "actual": cluster["Name"]
        },

        {
            "name": "Cluster State",
            "status": STATUS_PASS,
            "actual": cluster["Status"]["State"]
        },

        {
            "name": "Primary Node Capacity",
            "status": primary["status"],
            "actual":
                f"Running {primary['running']} / "
                f"Minimum {primary['minimum']}"
        },

        {
            "name": "Core Node Capacity",
            "status": core["status"],
            "actual":
                f"Running {core['running']} / "
                f"Minimum {core['minimum']}"
        },

        {
            "name": "Task Node Capacity",
            "status": task["status"],
            "actual":
                f"Running {task['running']} / "
                f"Minimum {task['minimum']}"
        },

        {
            "name": "Overall Cluster Health",
            "status":
                STATUS_PASS
                if overall_status == STATUS_HEALTHY
                else STATUS_FAIL,

            "actual": overall_status
        }

    ]


# ==============================================================================
# LOG HEALTH CHECKS
# ==============================================================================

def log_health_checks(result):
    """
    Writes all validation results to the Airflow log.
    """

    log_heading("Health Check Results")

    for check in result["checks"]:

        logger.info(
            "%-35s %-6s %s",
            check["name"],
            check["status"],
            check["actual"]
        )


# ==============================================================================
# MAIN HEALTH CHECK
# ==============================================================================

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

        # ------------------------------------------------------------------
        # Find Active Cluster
        # ------------------------------------------------------------------

        cluster = find_active_cluster(emr)

        if cluster is None:

            logger.warning(
                "No active EMR cluster found."
            )

            result = build_cluster_not_found_result()

            result["checks"] = [

                {
                    "name": "Active EMR Cluster",
                    "status": STATUS_FAIL,
                    "actual": "Cluster not found"
                },

                {
                    "name": "Overall Cluster Health",
                    "status": STATUS_FAIL,
                    "actual": STATUS_CLUSTER_NOT_FOUND
                }

            ]

            context["ti"].xcom_push(
                key="health_result",
                value=result
            )

            log_health_checks(result)

            return result

        # ------------------------------------------------------------------
        # Describe Cluster
        # ------------------------------------------------------------------

        cluster_details = describe_cluster(
            emr,
            cluster["Id"]
        )

        log_sub_heading("Cluster Details")

        log_key_value(
            "Cluster Name",
            cluster_details["Name"]
        )

        log_key_value(
            "Cluster ID",
            cluster_details["Id"]
        )

        log_key_value(
            "Cluster State",
            cluster_details["Status"]["State"]
        )

        # ------------------------------------------------------------------
        # Read Instance Groups
        # ------------------------------------------------------------------

        groups = get_instance_groups(
            emr,
            cluster["Id"]
        )

        # ------------------------------------------------------------------
        # Evaluate Health
        # ------------------------------------------------------------------

        result = evaluate_cluster_health(
            cluster_details,
            groups
        )

        # ------------------------------------------------------------------
        # Log Results
        # ------------------------------------------------------------------

        log_health_checks(result)

        # ------------------------------------------------------------------
        # Push XCom
        # ------------------------------------------------------------------

        context["ti"].xcom_push(
            key="health_result",
            value=result
        )

        log_heading("EMR HEALTH CHECK COMPLETED")

        return result

    except Exception:

        logger.exception(
            "Unexpected error while checking EMR health."
        )

        raise
  # ==============================================================================
# HTML HELPERS
# ==============================================================================

def build_badge(status):
    """
    Returns Outlook compatible PASS / FAIL badge.
    """

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


# ==============================================================================
# GENERIC TABLE
# ==============================================================================

def build_table(title, headers, rows):
    """
    Creates an Outlook compatible HTML table.
    """

    html = f"""
<h3 style="
font-family:{EMAIL_FONT};
color:{TEXT};
margin-top:25px;
margin-bottom:10px;
">
{title}
</h3>

<table
width="100%"
cellpadding="8"
cellspacing="0"
style="
border-collapse:collapse;
border:1px solid {BORDER};
font-family:{EMAIL_FONT};
font-size:13px;
">
"""

    html += "<tr>"

    for header in headers:

        html += f"""
<th style="
background:{GREY};
border:1px solid {BORDER};
text-align:left;
">
{header}
</th>
"""

    html += "</tr>"

    alternate = False

    for row in rows:

        colour = "#FFFFFF"

        if alternate:

            colour = "#FAFAFA"

        alternate = not alternate

        html += f'<tr style="background:{colour};">'

        for value in row:

            html += f"""
<td style="
border:1px solid {BORDER};
">
{value}
</td>
"""

        html += "</tr>"

    html += "</table>"

    return html


# ==============================================================================
# VALIDATION SUMMARY
# ==============================================================================

def validation_summary_rows(result):

    passed = len(
        [
            c for c in result["checks"]
            if c["status"] == STATUS_PASS
        ]
    )

    failed = len(result["checks"]) - passed

    return [

        ["Overall Status", result["overall_status"]],

        ["Checks Passed", passed],

        ["Checks Failed", failed],

        [
            "SMS Triggered",
            "Yes" if result["sms_sent"] else "No"
        ]

    ]


# ==============================================================================
# CLUSTER INFORMATION
# ==============================================================================

def cluster_information_rows(result):

    return [

        ["Cluster Name", result["cluster_name"]],

        ["Cluster ID", result["cluster_id"]],

        ["Cluster State", result["cluster_state"]],

        ["AWS Region", AWS_REGION],

        ["Environment", ENVIRONMENT],

        ["Report Time", result["report_time"]]

    ]


# ==============================================================================
# NODE CAPACITY
# ==============================================================================

def node_capacity_rows(result):

    rows = []

    for node in result["nodes"]:

        rows.append([

            node["group"],

            node["minimum"],

            node["maximum"],

            node["running"],

            build_badge(node["status"])

        ])

    return rows


# ==============================================================================
# HEALTH CHECKS
# ==============================================================================

def health_check_rows(result):

    rows = []

    for check in result["checks"]:

        rows.append([

            check["name"],

            build_badge(check["status"]),

            check["actual"]

        ])

    return rows


# ==============================================================================
# RECOMMENDED ACTIONS
# ==============================================================================

def recommended_actions_rows(result):

    if result["overall_status"] == STATUS_HEALTHY:

        return []

    return [

        [
            "High",
            "Verify the Jenkins EMR creation pipeline."
        ],

        [
            "High",
            "Verify the EMR cluster exists in AWS."
        ],

        [
            "Medium",
            "Review Core and Task instance groups."
        ],

        [
            "Medium",
            "Review EMR bootstrap logs."
        ]

    ]

# ==============================================================================
# BUILD EMAIL SUBJECT
# ==============================================================================

def build_email_subject(result):
    """
    Builds the email subject.
    """

    timestamp = result["report_time"]

    if result["overall_status"] == STATUS_HEALTHY:

        return (
            f"SUCCESS | EMR Healthy | "
            f"{result['cluster_name']} | "
            f"{timestamp}"
        )

    if result["overall_status"] == STATUS_CLUSTER_NOT_FOUND:

        return (
            f"CRITICAL | EMR Cluster Not Found | "
            f"{result['cluster_name']} | "
            f"{timestamp}"
        )

    return (
        f"CRITICAL | EMR Unhealthy | "
        f"{result['cluster_name']} | "
        f"{timestamp}"
    )


# ==============================================================================
# BUILD EMAIL HTML
# ==============================================================================

def build_email(result, context=None):
    """
    Builds the Outlook compatible HTML email.
    """

    # ----------------------------------------------------------------------
    # Banner
    # ----------------------------------------------------------------------

    if result["overall_status"] == STATUS_HEALTHY:

        banner_colour = GREEN

        summary = (
            "The EMR cluster is healthy. "
            "All instance groups satisfy the configured minimum "
            "running node requirements."
        )

    elif result["overall_status"] == STATUS_CLUSTER_NOT_FOUND:

        banner_colour = RED

        summary = (
            "No active EMR cluster was found. "
            "Verify the Jenkins EMR creation pipeline and "
            "confirm the cluster has been created."
        )

    else:

        banner_colour = RED

        summary = (
            "The EMR cluster is unhealthy. "
            "One or more instance groups are below the configured "
            "minimum running node count."
        )

    # ----------------------------------------------------------------------
    # Start HTML
    # ----------------------------------------------------------------------

    html = f"""
<!DOCTYPE html>

<html>

<head>

<meta charset="UTF-8">

<title>EMR Health Report</title>

</head>

<body style="
background:#F3F4F6;
padding:20px;
font-family:{EMAIL_FONT};
">

<table
width="{EMAIL_WIDTH}"
align="center"
cellpadding="20"
cellspacing="0"
style="
background:#FFFFFF;
border:1px solid {BORDER};
">

<tr>

<td>

<div style="
background:{banner_colour};
padding:25px;
color:white;
font-size:28px;
font-weight:bold;
">

AWS EMR Health Report

</div>

<p style="
font-size:15px;
margin-top:20px;
line-height:24px;
">

{summary}

</p>

"""

    # ----------------------------------------------------------------------
    # Validation Summary
    # ----------------------------------------------------------------------

    html += build_table(

        "Validation Summary",

        [

            "Metric",

            "Value"

        ],

        validation_summary_rows(result)

    )

    # ----------------------------------------------------------------------
    # Cluster Information
    # ----------------------------------------------------------------------

    html += build_table(

        "Cluster Information",

        [

            "Property",

            "Value"

        ],

        cluster_information_rows(result)

    )

    # ----------------------------------------------------------------------
    # Node Capacity
    # ----------------------------------------------------------------------

    if result["overall_status"] != STATUS_CLUSTER_NOT_FOUND:

        html += build_table(

            "Node Capacity",

            [

                "Group",

                "Minimum",

                "Maximum",

                "Running",

                "Status"

            ],

            node_capacity_rows(result)

        )

    # ----------------------------------------------------------------------
    # Health Checks
    # ----------------------------------------------------------------------

    html += build_table(

        "Health Check Results",

        [

            "Validation",

            "Result",

            "Actual Value"

        ],

        health_check_rows(result)

    )

    # ----------------------------------------------------------------------
    # Recommended Actions
    # ----------------------------------------------------------------------

    actions = recommended_actions_rows(result)

    if actions:

        html += build_table(

            "Recommended Actions",

            [

                "Priority",

                "Action"

            ],

            actions

        )

    # ----------------------------------------------------------------------
    # Airflow Information
    # ----------------------------------------------------------------------

    if context:

        airflow_rows = [

            [

                "DAG",

                context["dag"].dag_id

            ],

            [

                "Run ID",

                context["run_id"]

            ],

            [

                "Task",

                context["task"].task_id

            ],

            [

                "Execution Date",

                str(context["logical_date"])

            ]

        ]

        html += build_table(

            "Airflow Run Information",

            [

                "Property",

                "Value"

            ],

            airflow_rows

        )

    # ----------------------------------------------------------------------
    # Footer
    # ----------------------------------------------------------------------

    html += f"""

<hr style="margin-top:30px;">

<p style="
font-size:12px;
color:{SUB_TEXT};
line-height:20px;
">

<b>AWS EMR Health Monitor</b>

<br>

Environment :
{ENVIRONMENT}

<br>

Region :
{AWS_REGION}

<br>

Generated :
{result["report_time"]}

<br><br>

This is an automated notification generated by Apache Airflow (MWAA).

</p>

</td>

</tr>

</table>

</body>

</html>

"""

    return html

# ==============================================================================
# EMAIL NOTIFICATION
# ==============================================================================

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

    html_body = build_email(
        result=result,
        context=context
    )

    log_key_value("Recipients", ", ".join(EMAIL_RECIPIENTS))
    log_key_value("Subject", subject)

    try:

        send_email(
            to=EMAIL_RECIPIENTS,
            subject=subject,
            html_content=html_body
        )

        logger.info("Email sent successfully.")

        return True

    except Exception:

        logger.exception("Failed to send email.")

        return False


# ==============================================================================
# SNS SMS NOTIFICATION
# ==============================================================================

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
            f"CRITICAL: EMR Cluster '{CLUSTER_NAME}' "
            f"was not found. Please check your email."
        )

    else:

        message = (
            f"CRITICAL: EMR Cluster '{CLUSTER_NAME}' "
            f"is unhealthy. Please check your email."
        )

    try:

        response = sns.publish(

            TopicArn=SNS_TOPIC_ARN,

            Subject="EMR Health Alert",

            Message=message

        )

        result["sms_sent"] = True

        log_key_value(
            "SNS Message ID",
            response.get("MessageId")
        )

        logger.info("SMS sent successfully.")

        return True

    except Exception:

        logger.exception("Failed to send SMS.")

        return False


# ==============================================================================
# SEND NOTIFICATIONS
# ==============================================================================

def send_notifications(**context):
    """
    Sends Email and SMS notifications.

    Reads the health result from XCom.
    """

    log_heading("Notification Engine Started")

    ti = context["ti"]

    result = ti.xcom_pull(

        task_ids="check_emr_health",

        key="health_result"

    )

    if result is None:

        raise ValueError("Health result not found in XCom.")

    # ----------------------------------------------------------------------
    # Send Email
    # ----------------------------------------------------------------------

    email_success = send_email_notification(
        result,
        context
    )

    # ----------------------------------------------------------------------
    # Send SMS
    # ----------------------------------------------------------------------

    sms_success = send_sms_notification(
        result
    )

    # ----------------------------------------------------------------------
    # Notification Summary
    # ----------------------------------------------------------------------

    log_heading("Notification Summary")

    log_key_value(
        "Overall Status",
        result["overall_status"]
    )

    log_key_value(
        "Email Sent",
        email_success
    )

    log_key_value(
        "SMS Sent",
        sms_success
    )

    # ----------------------------------------------------------------------
    # Fail task only if both notifications failed
    # ----------------------------------------------------------------------

    if not email_success and not sms_success:

        raise Exception(
            "Both Email and SMS notifications failed."
        )

    logger.info("Notification Engine Completed Successfully.")

# ==============================================================================
# DAG DEFINITION
# ==============================================================================

with DAG(

    dag_id="emr_cluster_health_monitor",

    description="AWS EMR Cluster Health Monitoring",

    default_args=default_args,

    schedule="30 06 * * *",      # Update according to Jenkins schedule

    start_date=datetime(2026, 1, 1),

    catchup=False,

    max_active_runs=1,

    tags=[

        "AWS",

        "EMR",

        "Monitoring",

        "Production"

    ]

) as dag:

    # ----------------------------------------------------------------------
    # EMR Health Check
    # ----------------------------------------------------------------------

    health_check = PythonOperator(

        task_id="check_emr_health",

        python_callable=check_emr_health

    )

    # ----------------------------------------------------------------------
    # Notifications
    # ----------------------------------------------------------------------

    notifications = PythonOperator(

        task_id="send_notifications",

        python_callable=send_notifications

    )

    # ----------------------------------------------------------------------
    # Workflow
    # ----------------------------------------------------------------------

    health_check >> notifications
