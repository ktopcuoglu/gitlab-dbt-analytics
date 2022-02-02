""" This file contains common operators/functions to be used across multiple DAGs """
import os
import urllib.parse
from datetime import date, timedelta
from typing import List, Dict

from airflow.contrib.kubernetes.pod import Resources
from airflow.models import Variable
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

SSH_REPO = "git@gitlab.com:gitlab-data/analytics.git"
HTTP_REPO = "https://gitlab.com/gitlab-data/analytics.git"
DATA_IMAGE = "registry.gitlab.com/gitlab-data/data-image/data-image:v0.0.20"
DBT_IMAGE = "registry.gitlab.com/gitlab-data/data-image/dbt-image:v0.0.15"
PERMIFROST_IMAGE = "registry.gitlab.com/gitlab-data/permifrost:v0.13.1"
ANALYST_IMAGE = "registry.gitlab.com/gitlab-data/data-image/analyst-image:v0.0.23"

DATA_SCIENCE_SSH_REPO = "git@gitlab.com:gitlab-data/data-science.git"
DATA_SCIENCE_HTTP_REPO = "https://gitlab.com/gitlab-data/data-science.git"


def split_date_parts(day: date, partition: str) -> Dict:

    if partition == "month":
        split_dict = {
            "year": day.strftime("%Y"),
            "month": day.strftime("%m"),
            "part": day.strftime("%Y_%m"),
        }

    return split_dict


def partitions(from_date: date, to_date: date, partition: str) -> List[dict]:
    """
    A list of partitions to build.
    """

    delta = to_date - from_date
    all_parts = [
        split_date_parts((from_date + timedelta(days=i)), partition)
        for i in range(delta.days + 1)
    ]

    seen = set()
    parts = []
    # loops through every day and pulls out unique set of date parts
    for p in all_parts:
        if p["part"] not in seen:
            seen.add(p["part"])
            parts.append({k: v for k, v in p.items()})
    return parts


class MultiSlackChannelOperator:
    """
    Class that enables sending notifications to multiple Slack channels
    """

    def __init__(self, channels, context):
        self.channels = channels
        self.context = context

    def execute(self):
        attachment, slack_channel, task_id, task_text = slack_defaults(
            self.context, "failure"
        )
        for c in self.channels:
            slack_alert = SlackAPIPostOperator(
                attachments=attachment,
                channel=c,
                task_id=task_id,
                text=task_text,
                token=os.environ["SLACK_API_TOKEN"],
                username="Airflow",
            )

            slack_alert.execute()


def slack_defaults(context, task_type):
    """
    Function to handle switching between a task failure and success.
    """
    # base_url = "https://airflow.gitlabdata.com"
    base_url = "http://35.233.169.210:8080"
    execution_date = context["ts"]
    dag_context = context["dag"]
    dag_name = dag_context.dag_id
    dag_id = context["dag"].dag_id
    task_name = context["task"].task_id
    task_id = context["task_instance"].task_id
    execution_date_value = context["execution_date"]
    execution_date_epoch = execution_date_value.strftime("%s")
    execution_date_pretty = execution_date_value.strftime(
        "%a, %b %d, %Y at %-I:%M %p UTC"
    )

    # Generate the link to the logs
    log_params = urllib.parse.urlencode(
        {"dag_id": dag_id, "task_id": task_id, "execution_date": execution_date}
    )
    log_link = f"{base_url}/log?{log_params}"
    log_link_markdown = f"<{log_link}|View Logs>"

    if task_type == "success":
        if task_name == "snowflake-password-reset":
            slack_channel = "#data-lounge"
        else:
            slack_channel = dag_context.params.get(
                "slack_channel_override", "#analytics-pipelines"
            )

        color = "#1aaa55"
        fallback = "An Airflow DAG has succeeded!"
        task_id = "slack_succeeded"
        task_text = "Task succeeded!"

    if task_type == "failure":
        if task_name == "monitor-dbt-source-freshness":
            slack_channel = "#analytics-pipelines"
        else:
            slack_channel = dag_context.params.get(
                "slack_channel_override", "#analytics-pipelines"
            )
        color = "#a62d19"
        fallback = "An Airflow DAG has failed!"
        task_id = "slack_failed"
        task_text = "Task failure!"

    attachment = [
        {
            "mrkdwn_in": ["title", "value"],
            "color": color,
            "fallback": fallback,
            "fields": [
                {"title": "DAG", "value": dag_name, "short": True},
                {"title": "Task", "value": task_name, "short": True},
                {"title": "Logs", "value": log_link_markdown, "short": True},
                {"title": "Timestamp", "value": execution_date_pretty, "short": True},
            ],
            "footer": "Airflow",
            "footer_icon": "https://airflow.gitlabdata/static/pin_100.png",
            "ts": execution_date_epoch,
        }
    ]
    return attachment, slack_channel, task_id, task_text


def slack_snapshot_failed_task(context):
    """
    Function to be used as a callable for on_failure_callback for dbt-snapshots
    Send a Slack alert to #analytics-pipelines
    """
    multi_channel_alert = MultiSlackChannelOperator(
        channels=["#analytics-pipelines"], context=context
    )

    return multi_channel_alert.execute()


def slack_webhook_conn(slack_channel):
    slack_webhook = Variable.get("AIRFLOW_VAR_ANALYTICS_PIPELINES")

    airflow_http_con_id = Variable.get("AIRFLOW_VAR_SLACK_CONNECTION")
    return airflow_http_con_id, slack_webhook


def slack_failed_task(context):
    """
    Function to be used as a callable for on_failure_callback.
    Send a Slack alert.
    """

    attachment, slack_channel, task_id, task_text = slack_defaults(context, "failure")
    airflow_http_con_id, slack_webhook = slack_webhook_conn(slack_channel)

    slack_alert = SlackWebhookOperator(
        attachments=attachment,
        channel="#analytics-pipelines",
        task_id=task_id,
        message=task_text,
        http_conn_id=airflow_http_con_id,
        webhook_token=slack_webhook,
        username="Airflow",
    )
    return slack_alert.execute(context=None)


def slack_succeeded_task(context):
    """
    Function to be used as a callable for on_success_callback.
    Send a Slack alert.
    """

    attachment, slack_channel, task_id, task_text = slack_defaults(context, "success")
    airflow_http_con_id, slack_webhook = slack_webhook_conn(slack_channel)

    slack_alert = SlackWebhookOperator(
        attachments=attachment,
        channel="#analytics-pipelines",
        task_id=task_id,
        message=task_text,
        http_conn_id=airflow_http_con_id,
        webhook_token=slack_webhook,
        username="Airflow",
    )
    return slack_alert.execute(context=None)


# Set the resources for the task pods
pod_resources = {"request_memory": "1Gi", "request_cpu": "500m"}

# GitLab default settings for all DAGs
gitlab_defaults = dict(
    get_logs=True,
    image_pull_policy="Always",
    in_cluster=not os.environ["IN_CLUSTER"] == "False",
    is_delete_operator_pod=True,
    namespace=os.environ["NAMESPACE"],
    resources=pod_resources,
    cmds=["/bin/bash", "-c"],
)

# GitLab default environment variables for worker pods
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
gitlab_pod_env_vars = {
    "CI_PROJECT_DIR": "/analytics",
    "EXECUTION_DATE": "{{ next_execution_date }}",
    "SNOWFLAKE_PREPARATION_SCHEMA": "preparation",
    "SNOWFLAKE_SNAPSHOT_DATABASE": "RAW"
    if GIT_BRANCH == "master"
    else f"{GIT_BRANCH.upper()}_RAW",
    "SNOWFLAKE_LOAD_DATABASE": "RAW"
    if GIT_BRANCH == "master"
    else f"{GIT_BRANCH.upper()}_RAW",
    "SNOWFLAKE_PREP_DATABASE": "PREP"
    if GIT_BRANCH == "master"
    else f"{GIT_BRANCH.upper()}_PREP",
    "SNOWFLAKE_PROD_DATABASE": "PROD"
    if GIT_BRANCH == "master"
    else f"{GIT_BRANCH.upper()}_PROD",
}

# git commands
data_test_ssh_key_cmd = f"""
    export DATA_TEST_BRANCH="main" &&
    export DATA_SIREN_BRANCH="master" &&
    mkdir ~/.ssh/ &&
    touch ~/.ssh/id_rsa && touch ~/.ssh/config &&
    echo "$GIT_DATA_TESTS_PRIVATE_KEY" > ~/.ssh/id_rsa && chmod 0400 ~/.ssh/id_rsa &&
    echo "$GIT_DATA_TESTS_CONFIG" > ~/.ssh/config"""

clone_repo_cmd = f"""
    {data_test_ssh_key_cmd} &&
    if [[ -z "$GIT_COMMIT" ]]; then
        export GIT_COMMIT="HEAD"
    fi
    if [[ -z "$GIT_DATA_TESTS_PRIVATE_KEY" ]]; then
        export REPO="{HTTP_REPO}";
        else
        export REPO="{SSH_REPO}";
    fi &&
    echo "git clone -b {GIT_BRANCH} --single-branch --depth 1 $REPO" &&
    git clone -b {GIT_BRANCH} --single-branch --depth 1 $REPO &&
    echo "checking out commit $GIT_COMMIT" &&
    cd analytics &&
    git checkout $GIT_COMMIT &&
    cd .."""

clone_repo_sha_cmd = f"""
    {data_test_ssh_key_cmd} &&
    mkdir analytics &&
    cd analytics &&
    git init &&
    git remote add origin {SSH_REPO} &&
    echo "Fetching commit $GIT_COMMIT" &&
    git fetch origin --quiet &&
    git checkout $GIT_COMMIT"""

# extract command
clone_and_setup_extraction_cmd = f"""
    {clone_repo_cmd} &&
    export PYTHONPATH="$CI_PROJECT_DIR/orchestration/:$PYTHONPATH" &&
    cd analytics/extract/"""

# dbt commands
clone_and_setup_dbt_cmd = f"""
    {clone_repo_sha_cmd} &&
    cd transform/snowflake-dbt/"""

dbt_install_deps_cmd = f"""
    {clone_and_setup_dbt_cmd} &&
    dbt deps --profiles-dir profile"""

dbt_install_deps_and_seed_cmd = f"""
    {dbt_install_deps_cmd} &&
    dbt seed --profiles-dir profile --target prod --full-refresh"""

clone_and_setup_dbt_nosha_cmd = f"""
    {clone_repo_cmd} &&
    cd analytics/transform/snowflake-dbt/"""

dbt_install_deps_nosha_cmd = f"""
    {clone_and_setup_dbt_nosha_cmd} &&
    dbt deps --profiles-dir profile"""

dbt_install_deps_and_seed_nosha_cmd = f"""
    {dbt_install_deps_nosha_cmd} &&
    dbt seed --profiles-dir profile --target prod --full-refresh"""

clone_data_science_repo_cmd = f"""
    {data_test_ssh_key_cmd} &&
    if [[ -z "$GIT_COMMIT" ]]; then
        export GIT_COMMIT="HEAD"
    fi
    if [[ -z "$GIT_DATA_TESTS_PRIVATE_KEY" ]]; then
        export REPO="{DATA_SCIENCE_HTTP_REPO}";
        else
        export REPO="{DATA_SCIENCE_SSH_REPO}";
    fi &&
    echo "git clone -b main --single-branch --depth 1 $REPO" &&
    git clone -b main --single-branch --depth 1 $REPO &&
    echo "checking out commit $GIT_COMMIT" &&
    cd data-science &&
    git checkout $GIT_COMMIT &&
    cd .."""


def number_of_dbt_threads_argument(number_of_threads):
    return f"--threads {number_of_threads}"
