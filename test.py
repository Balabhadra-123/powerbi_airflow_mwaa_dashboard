from datetime import datetime
import boto3
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

# ============================================================
# CONFIGURATION
# ============================================================

CLUSTER_NAME = "emr-etl-test"
AWS_REGION = "ap-southeast-2"

logger = logging.getLogger(__name__)


# ============================================================
# HEALTH CHECK FUNCTION
# ============================================================

def check_emr_health(**context):

    emr = boto3.client(
        "emr",
        region_name=AWS_REGION
    )

    active_states = [
        "STARTING",
        "BOOTSTRAPPING",
        "RUNNING",
        "WAITING"
    ]

    clusters = emr.list_clusters(
        ClusterStates=active_states
    )

    cluster = None

    for c in clusters["Clusters"]:
        if c["Name"] == CLUSTER_NAME:
            cluster = c
            break

    # --------------------------------------------------------
    # CLUSTER NOT FOUND
    # --------------------------------------------------------

    if not cluster:

        result = {
            "status": "CLUSTER_NOT_FOUND",
            "cluster_name": CLUSTER_NAME,
            "cluster_id": None,
            "cluster_state": None,
            "failure_reason": "No active cluster found"
        }

        logger.error(result)

        context["ti"].xcom_push(
            key="health_result",
            value=result
        )

        return result

    cluster_id = cluster["Id"]
    cluster_state = cluster["Status"]["State"]

    # --------------------------------------------------------
    # INSTANCE GROUPS
    # --------------------------------------------------------

    response = emr.list_instance_groups(
        ClusterId=cluster_id
    )

    groups = {
        g["InstanceGroupType"]: g
        for g in response["InstanceGroups"]
    }

    # --------------------------------------------------------
    # MASTER
    # --------------------------------------------------------

    master_running = groups["MASTER"]["RunningInstanceCount"]

    master_status = (
        "PASS"
        if master_running == 1
        else "FAIL"
    )

    # --------------------------------------------------------
    # CORE
    # --------------------------------------------------------

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

    core_status = (
        "PASS"
        if core_running >= core_min
        else "FAIL"
    )

    # --------------------------------------------------------
    # TASK
    # --------------------------------------------------------

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

    task_status = (
        "PASS"
        if task_running >= task_min
        else "FAIL"
    )

    # --------------------------------------------------------
    # OVERALL STATUS
    # --------------------------------------------------------

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

    logger.info("===================================")
    logger.info("EMR HEALTH CHECK RESULT")
    logger.info(result)
    logger.info("===================================")

    context["ti"].xcom_push(
        key="health_result",
        value=result
    )

    return result


# ============================================================
# PRINT RESULT
# ============================================================

def print_health_result(**context):

    result = context["ti"].xcom_pull(
        task_ids="check_emr_health",
        key="health_result"
    )

    logger.info(result)


# ============================================================
# DAG
# ============================================================

default_args = {
    "owner": "airflow"
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

    print_result = PythonOperator(
        task_id="print_result",
        python_callable=print_health_result
    )

    health_check >> print_result
