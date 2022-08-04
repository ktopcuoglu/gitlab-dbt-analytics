"""
Propensity to Purchase Trials DAG
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    ANALYST_IMAGE,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
    data_test_ssh_key_cmd,
)
from kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_DATA_SCIENCE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    GITLAB_ANALYTICS_PRIVATE_TOKEN,
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
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2022, 7, 16),
    "dagrun_timeout": timedelta(hours=2),
}

# Prepare the cmd
DATA_SCIENCE_PTP_SSH_REPO = (
    "git@gitlab.com:gitlab-data/data-science-projects/propensity-to-purchase.git"
)
DATA_SCIENCE_PTP_HTTP_REPO = "https://gitlab_analytics:$GITLAB_ANALYTICS_PRIVATE_TOKEN@gitlab.com/gitlab-data/data-science-projects/propensity-to-purchase.git"

clone_data_science_ptp_repo_cmd = f"""
    {data_test_ssh_key_cmd} &&
    if [[ -z "$GIT_COMMIT" ]]; then
        export GIT_COMMIT="HEAD"
    fi
    if [[ -z "$GIT_DATA_TESTS_PRIVATE_KEY" ]]; then
        export REPO="{DATA_SCIENCE_PTP_HTTP_REPO}";
        else
        export REPO="{DATA_SCIENCE_PTP_SSH_REPO}";
    fi &&
    echo "git clone -b main --single-branch --depth 1 $REPO" &&
    git clone -b main --single-branch --depth 1 $REPO &&
    echo "checking out commit $GIT_COMMIT" &&
    cd propensity-to-purchase &&
    git checkout $GIT_COMMIT &&
    cd .."""

# Create the DAG
# Run Every Monday
dag = DAG(
    "propensity_to_purchase_trial",
    default_args=default_args,
    schedule_interval="0 12 * * 1",
)

# Task 1
ptpt_scoring_command = f"""
    {clone_data_science_ptp_repo_cmd} &&
    cd propensity-to-purchase/prod &&
    papermill scoring_code.ipynb -p is_local_development False
"""
KubernetesPodOperator(
    **gitlab_defaults,
    image=ANALYST_IMAGE,
    task_id="propensity-to-purchase-trial",
    name="propensity-to-purchase-trial",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_PASSWORD,
        SNOWFLAKE_DATA_SCIENCE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        GITLAB_ANALYTICS_PRIVATE_TOKEN,
    ],
    env_vars=pod_env_vars,
    arguments=[ptpt_scoring_command],
    dag=dag,
)
