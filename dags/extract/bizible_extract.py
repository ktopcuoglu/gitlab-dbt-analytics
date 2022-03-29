import logging
import os
from datetime import datetime, timedelta
import yaml
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from kubernetes_helpers import get_affinity, get_toleration
from airflow_utils import (
    DATA_IMAGE,
    clone_and_setup_extraction_cmd,
    gitlab_defaults,
    slack_failed_task,
    gitlab_pod_env_vars,
)
from kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    BIZIBLE_SNOWFLAKE_DATABASE,
    BIZIBLE_SNOWFLAKE_ROLE,
    BIZIBLE_SNOWFLAKE_PASSWORD,
    BIZIBLE_SNOWFLAKE_USER,
    BIZIBLE_SNOWFLAKE_WAREHOUSE,
    BIZIBLE_SNOWFLAKE_ACCOUNT,
)

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
pod_env_vars = gitlab_pod_env_vars

logging.info(pod_env_vars)
# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=12),
    "sla_miss_callback": slack_failed_task,
    "start_date": datetime(2019, 1, 1),
    "dagrun_timeout": timedelta(hours=6),
}

# Create the DAG
dag = DAG(
    "bizible_extract",
    default_args=default_args,
    concurrency=2,
    schedule_interval="25 */11 * * *",
)


def extract_manifest(file_path):
    with open(file_path, "r") as file:
        manifest_dict = yaml.load(file, Loader=yaml.FullLoader)
    return manifest_dict


manifest = extract_manifest(
    "analytics/extract/bizible/manifests/el_bizible_tables.yaml"
)
tables = manifest.get("tables")

for table_name in tables:
    # Bizible Extract
    extract_command = f"""
        {clone_and_setup_extraction_cmd} &&
            python bizible/src/main.py tap bizible/manifests/el_bizible_tables.yaml --load_only_table {table_name}
    """
    task_identifier = f"bizible-extract-{table_name.replace('_', '-')}"
    # having both xcom flag flavors since we're in an airflow version where one is being deprecated
    bizible_extract = KubernetesPodOperator(
        **gitlab_defaults,
        image=DATA_IMAGE,
        task_id=task_identifier,
        name=task_identifier,
        secrets=[
            SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_LOAD_PASSWORD,
            SNOWFLAKE_LOAD_ROLE,
            SNOWFLAKE_LOAD_USER,
            SNOWFLAKE_LOAD_WAREHOUSE,
            BIZIBLE_SNOWFLAKE_DATABASE,
            BIZIBLE_SNOWFLAKE_ROLE,
            BIZIBLE_SNOWFLAKE_PASSWORD,
            BIZIBLE_SNOWFLAKE_USER,
            BIZIBLE_SNOWFLAKE_WAREHOUSE,
            BIZIBLE_SNOWFLAKE_ACCOUNT,
        ],
        env_vars={
            **pod_env_vars,
            "TASK_INSTANCE": "{{ task_instance_key_str }}",
            "task_id": task_identifier,
        },
        affinity=get_affinity(False),
        tolerations=get_toleration(False),
        arguments=[extract_command],
        dag=dag,
    )
