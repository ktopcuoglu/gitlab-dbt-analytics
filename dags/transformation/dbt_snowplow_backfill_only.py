import json
import os
from datetime import date, datetime

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow_utils import (
    DBT_IMAGE,
    dbt_install_deps_nosha_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    partitions,
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
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
)

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {**gitlab_pod_env_vars, **{}}
task_secrets = [
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
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
]

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "params": {"slack_channel_override": "#dbt-runs"},
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1, 0, 0, 0),
}

# Create the DAG
dag = DAG(
    "dbt_snowplow_backfill_only", default_args=default_args, schedule_interval=None
)


def generate_dbt_command(vars_dict):
    json_dict = json.dumps(vars_dict)

    dbt_generate_command = f""" 
        {dbt_install_deps_nosha_cmd} &&
        export SNOWFLAKE_TRANSFORM_WAREHOUSE="TRANSFORMING_4XL" &&
        dbt run --profiles-dir profile --target prod --models +snowplow --full-refresh --vars '{json_dict}'; ret=$?;
        python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
        """

    return KubernetesPodOperator(
        **gitlab_defaults,
        image=DBT_IMAGE,
        task_id=f"dbt-snowplow-backfill-{vars_dict['year']}-{vars_dict['month']}",
        name=f"dbt-snowplow-backfill-{vars_dict['year']}-{vars_dict['month']}",
        secrets=task_secrets,
        env_vars=pod_env_vars,
        arguments=[dbt_generate_command],
        dag=dag,
    )


dummy_operator = DummyOperator(task_id="start", dag=dag)

for month in partitions(
    datetime.strptime("2018-07-01", "%Y-%m-%d").date(), date.today(), "month"
):
    dummy_operator >> generate_dbt_command(month)
