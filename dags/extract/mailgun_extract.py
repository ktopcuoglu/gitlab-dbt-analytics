import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
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
    MAILGUN_API_KEY,
)

from kubernetes_helpers import get_affinity, get_toleration

env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {**gitlab_pod_env_vars, **{}}

default_args = {
    "catchup": True,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=24),
    "sla_miss_callback": slack_failed_task,
    "start_date": datetime(2020, 1, 1),
}

dag = DAG(
    "mailgun_extract",
    default_args=default_args,
    schedule_interval="0 */12 * * *",
)

events = [
    "rejected",
    "delivered",
    "failed",
    "opened",
    "clicked",
    "unsubscribed",
    "complained",
]

for e in events:
    # don't add a newline at the end of this because it gets added to in the K8sPodOperator arguments
    mailgun_extract_command = (
        f"{clone_and_setup_extraction_cmd} && "
        f"python mailgun/src/execute.py load_event_logs --event {e} --full_refresh False"
    )

    mailgun_operator = KubernetesPodOperator(
        **gitlab_defaults,
        image=DATA_IMAGE,
        task_id=f"mailgun-extract-{e}",
        name=f"mailgun-extract-{e}",
        secrets=[
            SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_LOAD_ROLE,
            SNOWFLAKE_LOAD_USER,
            SNOWFLAKE_LOAD_WAREHOUSE,
            SNOWFLAKE_LOAD_PASSWORD,
            MAILGUN_API_KEY,
        ],
        env_vars={
            **pod_env_vars,
            "START_TIME": "{{ execution_date.isoformat() }}",
            "END_TIME": "{{ yesterday_ds }}",
        },
        affinity=get_affinity(False),
        tolerations=get_toleration(False),
        arguments=[mailgun_extract_command],
        dag=dag,
    )
