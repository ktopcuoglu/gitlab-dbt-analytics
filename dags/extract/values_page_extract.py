import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DATA_IMAGE,
    clone_and_setup_extraction_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
)
from kube_secrets import (
    GCP_SERVICE_CREDS,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
)
from kubernetes_helpers import get_affinity, get_toleration

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
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2019, 1, 1),
    "dagrun_timeout": timedelta(hours=2),
}

# Set the command for the container
container_cmd = f"""
    {clone_and_setup_extraction_cmd} &&
    cd /usr/local/ && 
    mkdir -p gitlab && 
    cd gitlab && 
    git init && 
    git remote add origin https://gitlab.com/gitlab-com/www-gitlab-com.git && 
    git checkout -b master && 
    git config core.sparsecheckout true && 
    echo sites/handbook/source/handbook/values/ >> .git/info/sparse-checkout && 
    cat .git/info/sparse-checkout &&
    echo "git pull origin master" &&
    git pull origin master &&
    git log --pretty="format:%H,%cN,%ci,%s" sites/handbook/source/handbook/values/index.html.md >> values.csv
    python3 /usr/local/analytics/extract/sheetload/sheetload.py csv --filename values.csv --schema git_log --tablename values_page --header None
"""

# Create the DAG
dag = DAG(
    "value_page_extract", default_args=default_args, schedule_interval="0 2 */1 * *"
)

# Task 1
sheetload_run = KubernetesPodOperator(
    **gitlab_defaults,
    image="registry.gitlab.com/gitlab-data/data-image/data-image:v0.0.12",
    task_id="value-page-extract",
    name="value-page-extract",
    secrets=[
        GCP_SERVICE_CREDS,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_PASSWORD,
    ],
    affinity=get_affinity(False),
    tolerations=get_toleration(False),
    env_vars=pod_env_vars,
    arguments=[container_cmd],
    dag=dag,
)
