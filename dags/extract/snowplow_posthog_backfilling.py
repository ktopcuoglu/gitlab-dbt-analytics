"""
DAG for Snowplow -> PostHog backfilling
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.models import Variable
from airflow_utils import (
    clone_repo_cmd,
    DATA_IMAGE,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
)
from kube_secrets import (
    GIT_DATA_TESTS_PRIVATE_KEY,
    GIT_DATA_TESTS_CONFIG,
    POSTHOG_AWS_ACCESS_KEY_ID,
    POSTHOG_AWS_SECRET_ACCESS_KEY,
    POSTHOG_AWS_S3_SNOWPLOW_BUCKET,
    POSTHOG_PROJECT_API_KEY,
    POSTHOG_PERSONAL_API_KEY,
    POSTHOG_HOST,
)


# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {**gitlab_pod_env_vars, **{}}


task_secrets = [
    GIT_DATA_TESTS_PRIVATE_KEY,
    GIT_DATA_TESTS_CONFIG,
    POSTHOG_AWS_ACCESS_KEY_ID,
    POSTHOG_AWS_SECRET_ACCESS_KEY,
    POSTHOG_AWS_S3_SNOWPLOW_BUCKET,
    POSTHOG_PROJECT_API_KEY,
    POSTHOG_PERSONAL_API_KEY,
    POSTHOG_HOST,
]

start_date = datetime.strptime(
    Variable.get("POSTHOG_BACKFILL_START_DATE", default_var="2022-06-01"), "%Y-%m-%d"
)
end_date = datetime.strptime(
    Variable.get("POSTHOG_BACKFILL_END_DATE", default_var="2021-06-20"), "%Y-%m-%d"
)

DAG_NAME = "snowplow_posthog_backfill"
SCHEMA_NAME = "gitlab_events"

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "start_date": start_date,
}


# Create the DAG
def generate_task_command(vars_dict: dict, dag_name: str):
    """
    Generate generic command separated per time frame
    to create tasks
    """

    generated_command = f"""
    {clone_repo_cmd} &&
    cd analytics/extract/snowplow_posthog/ &&
    python3 backfill.py snowplow_posthog_backfill --day {vars_dict['year']}{vars_dict['month']}{vars_dict['day']}
"""

    return KubernetesPodOperator(
        **gitlab_defaults,
        image="registry.gitlab.com/gitlab-data/data-image/data-image:v1.0.3",
        task_id=f"{dag_name}-{vars_dict['year']}-{vars_dict['month']}-{vars_dict['day']}",
        name=f"{dag_name}-{vars_dict['year']}-{vars_dict['month']}-{vars_dict['day']}",
        secrets=task_secrets,
        env_vars=pod_env_vars,
        arguments=[generated_command],
        dag=dag,
    )


dag = DAG(
    DAG_NAME,
    default_args=default_args,
    schedule_interval=None,
    concurrency=3,
)


def get_date_range(from_date: datetime, to_date: datetime) -> list:
    """
    Get the list of dict partitioned year/month/day to slice tasks per day
    Usage:
    ------------------------------------------------------------------
    Input: 2022-06-01 - 2022-06-02
    Output: [{'year': '2022', 'month': '06', 'day': '01'},
            {'year': '2022', 'month': '06', 'day': '02'}]
    ------------------------------------------------------------------
    """
    ret_list = []

    delta = to_date - from_date

    for i in range(delta.days + 1):

        curr_date = from_date + timedelta(days=i)
        partition = dict(
            year=curr_date.strftime("%Y"),
            month=curr_date.strftime("%m"),
            day=curr_date.strftime("%d"),
        )

        ret_list.append(partition)

    return ret_list


for date_range in get_date_range(start_date.date(), end_date.date()):
    generate_task_command(vars_dict=date_range, dag_name=DAG_NAME)
