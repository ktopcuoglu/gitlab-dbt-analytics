import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
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
    GITLAB_ANALYTICS_PRIVATE_TOKEN,
    GITLAB_COM_API_TOKEN,
)
from kubernetes_helpers import get_affinity, get_toleration

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
pod_env_vars = gitlab_pod_env_vars

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
    "dagrun_timeout": timedelta(hours=2),
}

# Create the DAG
dag = DAG(
    "gitlab_data_yaml_extract",
    default_args=default_args,
    schedule_interval="0 */8 * * *",
)

# YAML Extract
data_yaml_extract_cmd = f"""{clone_and_setup_extraction_cmd} && python gitlab_data_yaml/upload.py"""
data_yaml_extract = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="data-yaml-extract",
    name="data-yaml-extract",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_PASSWORD,
        GITLAB_ANALYTICS_PRIVATE_TOKEN,
        GITLAB_COM_API_TOKEN,
    ],
    affinity=get_affinity(False),
    tolerations=get_toleration(False),
    env_vars=pod_env_vars,
    arguments=[data_yaml_extract_cmd],
    dag=dag,
)

# Flaky tests extract
flaky_tests_extract_cmd = f"""{clone_and_setup_extraction_cmd} && python gitlab_flaky_tests/upload.py"""

flaky_tests_extract = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="flaky-tests-extract",
    name="flaky-tests-extract",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_PASSWORD,
        GITLAB_ANALYTICS_PRIVATE_TOKEN,
        GITLAB_COM_API_TOKEN,
    ],
    affinity=get_affinity(False),
    tolerations=get_toleration(False),
    env_vars=pod_env_vars,
    arguments=[flaky_tests_extract_cmd],
    dag=dag,
)

# Feature flags extract
feature_flags_extract_cmd = f"""{clone_and_setup_extraction_cmd} && python gitlab_feature_flags_yaml/upload.py"""

feature_flags_extract = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="feature-flags-extract",
    name="feature-flags-extract",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_PASSWORD,
        GITLAB_ANALYTICS_PRIVATE_TOKEN,
        GITLAB_COM_API_TOKEN,
    ],
    affinity=get_affinity(False),
    tolerations=get_toleration(False),
    env_vars=pod_env_vars,
    arguments=[feature_flags_extract_cmd],
    dag=dag,
)

data_yaml_extract >> flaky_tests_extract >> feature_flags_extract