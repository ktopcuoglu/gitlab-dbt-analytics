import os
from os import environ as env
from datetime import datetime, timedelta
from yaml import load, safe_load, YAMLError
from fire import Fire

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import DATA_IMAGE, clone_repo_cmd, gitlab_defaults, slack_failed_task
from kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_DATABASE,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_USER,
)

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {
    "CI_PROJECT_DIR": "/analytics",
    "SNOWFLAKE_PROD_DATABASE": "PROD",
}

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2021, 3, 18),
    "dagrun_timeout": timedelta(hours=1),
}

# Create the DAG
dag = DAG("data_pumps", default_args=default_args, schedule_interval="0 5 * * *")

airflow_home = env["AIRFLOW_HOME"]

with open(f"{airflow_home}/analytics/pump/pumps.yml", "r") as file:
    try:
        stream = safe_load(file)
    except YAMLError as exc:
        print(exc)

    # there has to be a better way to do this
    pumps = [(pump) for pump in stream["pumps"]]

# Loop through pumps to create tasks

for pump_model in pumps:

    run_pumps_command = f"""
      {clone_repo_cmd} &&
      python pump/pumps.py \
        --model={pump_model["model"]} \
        --sensitive={pump_model["sensitive"]} \
        --timestamp={pump_model["timestamp"]} \
        --inc_start="2021-02-26" \
        --inc_end="2021-02-27"  \
        /
  """

    run_pumps = KubernetesPodOperator(
        **gitlab_defaults,
        image=DATA_IMAGE,
        task_id=pump_model["model"],
        name=pump_model["model"],
        secrets=[
            SNOWFLAKE_USER,
            SNOWFLAKE_PASSWORD,
            SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_LOAD_DATABASE,
            SNOWFLAKE_LOAD_WAREHOUSE,
        ],
        env_vars=pod_env_vars,
        arguments=[run_pumps_command],
        dag=dag,
    )
