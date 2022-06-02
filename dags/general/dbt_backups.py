"""
Backup DAG for handling snapshot housekeeping
"""

import os
from datetime import datetime, timedelta
import yaml

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DBT_IMAGE,
    dbt_install_deps_nosha_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
)
from kube_secrets import (
    GIT_DATA_TESTS_PRIVATE_KEY,
    GIT_DATA_TESTS_CONFIG,
    SALT,
    SALT_EMAIL,
    SALT_IP,
    SALT_NAME,
    SALT_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
)

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {**gitlab_pod_env_vars, **{}}


# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "sla": timedelta(hours=12),
    "sla_miss_callback": slack_failed_task,
    "start_date": datetime(2019, 1, 1, 0, 0, 0),
    "dagrun_timeout": timedelta(hours=8),
}

# Create the DAG. Run daily at 04:05
dag = DAG("dbt_backups", default_args=default_args, schedule_interval="5 4 * * *")


def generate_task(task: str, backup_list: list, is_included: bool = False) -> None:
    """
    Generate task separated per each table/snapshot
    """

    task_prefix = "dbt_backups"

    # dbt run-operation for backups
    args = f"""'{{TABLE_LIST_BACKUP: {backup_list}, INCLUDED: {is_included}}}'"""

    dbt_backups_cmd = f"""
        {dbt_install_deps_nosha_cmd} && export SNOWFLAKE_TRANSFORM_WAREHOUSE="BACKUPS_XS" &&
        dbt run-operation backup_to_gcs --args {args} --profiles-dir profile
    """

    dbt_backups = KubernetesPodOperator(
        **gitlab_defaults,
        image=DBT_IMAGE,
        task_id=f"{task_prefix}-{task}",
        name=f"{task_prefix}-{task}",
        secrets=[
            GIT_DATA_TESTS_PRIVATE_KEY,
            GIT_DATA_TESTS_CONFIG,
            SALT,
            SALT_EMAIL,
            SALT_IP,
            SALT_NAME,
            SALT_PASSWORD,
            SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_USER,
            SNOWFLAKE_PASSWORD,
            SNOWFLAKE_TRANSFORM_ROLE,
            SNOWFLAKE_TRANSFORM_WAREHOUSE,
            SNOWFLAKE_TRANSFORM_SCHEMA,
        ],
        env_vars=pod_env_vars,
        arguments=[dbt_backups_cmd],
        dag=dag,
    )


def load_manifest_file(file_name: str) -> dict:
    """
    Load manifest file with table/snapshot definitions
    """
    with open(file_name, "r", encoding="utf-8") as yaml_file:
        return yaml.load(yaml_file, Loader=yaml.FullLoader)


config_dict = load_manifest_file("analytics/dags/general/backup_manifest.yaml")

for task_name, task_details in config_dict.items():

    generate_task(
        task=task_name,
        backup_list=task_details.get("TABLE_LIST_BACKUP"),
        is_included=task_details.get("INCLUDED"),
    )
