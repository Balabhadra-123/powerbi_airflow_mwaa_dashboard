from datetime import datetime, timedelta
import boto3
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email

# ==========================================================
# CONFIG
# ==========================================================

CLUSTER_NAME = "emr-etl-test"

AWS_REGION = "ap-southeast-2"

EMAIL_RECIPIENTS = [
    "TODO@company.com"
]

SNS_TOPIC_ARN = "arn:aws:sns:ap-southeast-2:xxxxxxxxxxxx:TODO"

ACTIVE_STATES = [
    "STARTING",
    "BOOTSTRAPPING",
    "RUNNING",
    "WAITING"
]

logger = logging.getLogger(__name__)

# ==========================================================
# EMAIL TEMPLATES
# ==========================================================


def build_healthy_email(result):

    return f"""
    <html>
    <body style="font-family:Segoe UI,Arial,sans-serif;background:#f4f6f9;padding:20px;">

    <table width="850" align="center" cellpadding="0" cellspacing="0"
           style="background:white;border:1px solid #dfe3e8;">

        <tr>
            <td style="background:#198754;color:white;padding:20px;">
                <h2>✓ EMR Health Check Report</h2>
                <h3>Status : HEALTHY</h3>
            </td>
        </tr>

        <tr>
            <td style="padding:20px;">

                <h3>Cluster Information</h3>

                <table border="1" cellpadding="8"
                       style="border-collapse:collapse;width:100%;">
                    <tr>
                        <td><b>Cluster Name</b></td>
                        <td>{result['cluster_name']}</td>
                    </tr>
                    <tr>
                        <td><b>Cluster ID</b></td>
                        <td>{result['cluster_id']}</td>
                    </tr>
                    <tr>
                        <td><b>Cluster State</b></td>
                        <td>{result['cluster_state']}</td>
                    </tr>
                </table>

                <br>

                <h3>Node Capacity</h3>

                <table border="1" cellpadding="8"
                       style="border-collapse:collapse;width:100%;">

                    <tr>
                        <th>Node Group</th>
                        <th>Min</th>
                        <th>Max</th>
                        <th>Running</th>
                        <th>Status</th>
                    </tr>

                    <tr>
                        <td>Primary</td>
                        <td>1</td>
                        <td>1</td>
                        <td>{result['master']['running']}</td>
                        <td>PASS</td>
                    </tr>

                    <tr>
                        <td>Core</td>
                        <td>{result['core']['min']}</td>
                        <td>{result['core']['max']}</td>
                        <td>{result['core']['running']}</td>
                        <td>{result['core']['status']}</td>
                    </tr>

                    <tr>
                        <td>Task</td>
                        <td>{result['task']['min']}</td>
                        <td>{result['task']['max']}</td>
                        <td>{result['task']['running']}</td>
                        <td>{result['task']['status']}</td>
                    </tr>

                </table>

                <br>

                <div style="background:#e8f5e9;padding:15px;
                            border-left:5px solid #198754;">
                    EMR cluster is healthy and ready for MWAA workload execution.
                </div>

            </td>
        </tr>

    </table>

    </body>
    </html>
    """


def build_unhealthy_email(result):

    return f"""
    <html>
    <body style="font-family:Segoe UI,Arial,sans-serif;background:#f4f6f9;padding:20px;">

    <table width="850" align="center" cellpadding="0" cellspacing="0"
           style="background:white;border:1px solid #dfe3e8;">

        <tr>
            <td style="background:#dc3545;color:white;padding:20px;">
                <h2>✗ EMR Health Check Report</h2>
                <h3>Status : UNHEALTHY</h3>
            </td>
        </tr>

        <tr>
            <td style="padding:20px;">

                <h3>Cluster Information</h3>

                <table border="1" cellpadding="8"
                       style="border-collapse:collapse;width:100%;">
                    <tr>
                        <td><b>Cluster Name</b></td>
                        <td>{result['cluster_name']}</td>
                    </tr>
                    <tr>
                        <td><b>Cluster ID</b></td>
                        <td>{result['cluster_id']}</td>
                    </tr>
                    <tr>
                        <td><b>Cluster State</b></td>
                        <td>{result['cluster_state']}</td>
                    </tr>
                </table>

                <br>

                <h3>Node Capacity</h3>

                <table border="1" cellpadding="8"
                       style="border-collapse:collapse;width:100%;">

                    <tr>
                        <th>Node Group</th>
                        <th>Min</th>
                        <th>Max</th>
                        <th>Running</th>
                        <th>Status</th>
                    </tr>

                    <tr>
                        <td>Primary</td>
                        <td>1</td>
                        <td>1</td>
                        <td>{result['master']['running']}</td>
                        <td>{result['master']['status']}</td>
                    </tr>

                    <tr>
                        <td>Core</td>
                        <td>{result['core']['min']}</td>
                        <td>{result['core']['max']}</td>
                        <td>{result['core']['running']}</td>
                        <td>{result['core']['status']}</td>
                    </tr>

                    <tr>
                        <td>Task</td>
                        <td>{result['task']['min']}</td>
                        <td>{result['task']['max']}</td>
                        <td>{result['task']['running']}</td>
                        <td>{result['task']['status']}</td>
                    </tr>

                </table>

                <br>

                <div style="background:#fef2f2;padding:15px;
                            border-left:5px solid #dc3545;">
                    One or more node groups are below the configured minimum
                    threshold. Airflow workloads may fail.
                </div>

            </td>
        </tr>

    </table>

    </body>
    </html>
    """


def build_cluster_not_found_email():

    return f"""
    <html>
    <body style="font-family:Segoe UI,Arial,sans-serif;background:#f4f6f9;padding:20px;">

    <table width="850" align="center" cellpadding="0" cellspacing="0"
           style="background:white;border:1px solid #dfe3e8;">

        <tr>
            <td style="background:#dc3545;color:white;padding:20px;">
                <h2>✗ EMR Health Check Report</h2>
                <h3>Status : CLUSTER NOT FOUND</h3>
            </td>
        </tr>

        <tr>
            <td style="padding:20px;">

                <h3>Cluster Name</h3>

                <p>{CLUSTER_NAME}</p>

                <div style="background:#fef2f2;padding:15px;
                            border-left:5px solid #dc3545;">
                    No active EMR cluster was found.

                    Possible causes:
                    <ul>
                        <li>Jenkins pipeline failure</li>
                        <li>EMR provisioning failure</li>
                        <li>Unexpected cluster termination</li>
                    </ul>
                </div>

            </td>
        </tr>

    </table>

    </body>
    </html>
    """

# ==========================================================
# HEALTH CHECK
# ==========================================================


def check_emr_health(**context):

    emr = boto3.client(
        "emr",
        region_name=AWS_REGION
    )

    clusters = emr.list_clusters(
        ClusterStates=ACTIVE_STATES
    )

    cluster = None

    for c in clusters["Clusters"]:
        if c["Name"] == CLUSTER_NAME:
            cluster = c
            break

    if not cluster:

        result = {
            "status": "CLUSTER_NOT_FOUND"
        }

        context["ti"].xcom_push(
            key="health_result",
            value=result
        )

        return

    cluster_id = cluster["Id"]

    cluster_details = emr.describe_cluster(
        ClusterId=cluster_id
    )

    cluster_state = (
        cluster_details["Cluster"]
        ["Status"]
        ["State"]
    )

    groups_response = emr.list_instance_groups(
        ClusterId=cluster_id
    )

    groups = {
        g["InstanceGroupType"]: g
        for g in groups_response["InstanceGroups"]
    }

    master_running = (
        groups["MASTER"]["RunningInstanceCount"]
    )

    core_group = groups["CORE"]

    core_running = core_group["RunningInstanceCount"]

    core_min = (
        core_group["AutoScalingPolicy"]
        ["Constraints"]
        ["MinCapacity"]
    )

    core_max = (
        core_group["AutoScalingPolicy"]
        ["Constraints"]
        ["MaxCapacity"]
    )

    task_group = groups["TASK"]

    task_running = task_group["RunningInstanceCount"]

    task_min = (
        task_group["AutoScalingPolicy"]
        ["Constraints"]
        ["MinCapacity"]
    )

    task_max = (
        task_group["AutoScalingPolicy"]
        ["Constraints"]
        ["MaxCapacity"]
    )

    master_status = (
        "PASS"
        if master_running == 1
        else "FAIL"
    )

    core_status = (
        "PASS"
        if core_running >= core_min
        else "FAIL"
    )

    task_status = (
        "PASS"
        if task_running >= task_min
        else "FAIL"
    )

    overall_status = (
        "HEALTHY"
        if (
            master_status == "PASS"
            and core_status == "PASS"
            and task_status == "PASS"
        )
        else "UNHEALTHY"
    )

    result = {
        "status": overall_status,
        "cluster_name": CLUSTER_NAME,
        "cluster_id": cluster_id,
        "cluster_state": cluster_state,
        "master": {
            "running": master_running,
            "status": master_status
        },
        "core": {
            "running": core_running,
            "min": core_min,
            "max": core_max,
            "status": core_status
        },
        "task": {
            "running": task_running,
            "min": task_min,
            "max": task_max,
            "status": task_status
        }
    }

    logger.info(result)

    context["ti"].xcom_push(
        key="health_result",
        value=result
    )


# ==========================================================
# NOTIFICATION
# ==========================================================


def send_notification(**context):

    result = context["ti"].xcom_pull(
        task_ids="check_emr_health",
        key="health_result"
    )

    sns = boto3.client(
        "sns",
        region_name=AWS_REGION
    )

    if result["status"] == "HEALTHY":

        send_email(
            to=EMAIL_RECIPIENTS,
            subject=f"[SUCCESS] EMR Health Check Passed - {CLUSTER_NAME}",
            html_content=build_healthy_email(result)
        )

        return

    if result["status"] == "CLUSTER_NOT_FOUND":

        send_email(
            to=EMAIL_RECIPIENTS,
            subject=f"[CRITICAL] EMR Cluster Not Found - {CLUSTER_NAME}",
            html_content=build_cluster_not_found_email()
        )

        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=f"CRITICAL: EMR cluster {CLUSTER_NAME} not found. Check Jenkins EMR creation pipeline."
        )

        return

    send_email(
        to=EMAIL_RECIPIENTS,
        subject=f"[CRITICAL] EMR Health Check Failed - {CLUSTER_NAME}",
        html_content=build_unhealthy_email(result)
    )

    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=f"CRITICAL: EMR cluster {CLUSTER_NAME} unhealthy. Running nodes below configured minimum threshold."
    )

# ==========================================================
# DAG
# ==========================================================

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="emr_cluster_readiness_check",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule="45 5 * * *",
    default_args=default_args,
    tags=["emr", "healthcheck"]
) as dag:

    health_check = PythonOperator(
        task_id="check_emr_health",
        python_callable=check_emr_health
    )

    notification = PythonOperator(
        task_id="send_notification",
        python_callable=send_notification
    )

    health_check >> notification
